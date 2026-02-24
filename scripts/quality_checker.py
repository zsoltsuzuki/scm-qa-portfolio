import pandas as pd
import numpy as np
from datetime import datetime
from typing import Any


# Constants
VALID_STATUSES = {"Pending", "In Transit", "Delivered", "Cancelled", "Returned"}

EXPECTED_SCHEMA = {
    "order_id":        str,
    "product":         str,
    "quantity":        float,   # pandas reads int columns with nulls as float
    "unit_price_eur":  float,
    "supplier":        str,
    "region":          str,
    "order_date":      str,
    "delivery_date":   str,
    "status":          str,
    "warehouse_id":    str,
}

VALID_DATE_FORMATS = ["%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y", "%B %d, %Y"]


def _build_result(
    check_name: str,
    passed: bool,
    summary: str,
    details: dict[str, Any],
    affected_rows: list[int] | None = None,
) -> dict:
    """
    Standardised result dict returned by every check method.

    Keys:
        check       (str)  : name of the check
        passed      (bool) : True if no issues found
        summary     (str)  : human-readable one-liner
        details     (dict) : check-specific metrics and findings
        affected_rows (list[int]): DataFrame index positions of bad rows
        timestamp   (str)  : ISO timestamp of when the check ran
    """
    return {
        "check":         check_name,
        "passed":        passed,
        "summary":       summary,
        "details":       details,
        "affected_rows": affected_rows or [],
        "timestamp":     datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

class DataQualityChecker:
    """
    Performs automated data quality checks on an SCM orders CSV.

    Attributes:
        filepath (str)          : path to the CSV file
        df       (pd.DataFrame) : loaded dataset (None until load() is called)
        _results (list[dict])   : accumulated check results
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.df: pd.DataFrame | None = None
        self._results: list[dict] = []

#Setup
    def load(self) -> None:
        """Loads the CSV into a DataFrame. Must be called before any checks."""
        try:
            self.df = pd.read_csv(self.filepath, dtype=str)  # load all as str first
        except FileNotFoundError:
            raise FileNotFoundError(f"Dataset not found at path: '{self.filepath}'")
        except pd.errors.EmptyDataError:
            raise ValueError(f"Dataset at '{self.filepath}' is empty — nothing to check.")
        except pd.errors.ParserError as e:
            raise ValueError(f"Failed to parse CSV at '{self.filepath}': {e}")

        if self.df.empty:
            raise ValueError(f"Dataset at '{self.filepath}' has no rows after loading.")

        # Cast numeric columns where possible, keeping NaN for missing
        for col in ["quantity", "unit_price_eur"]:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

        print(f"Loaded '{self.filepath}' — {len(self.df)} rows, {len(self.df.columns)} columns.")
        
    def _require_loaded(self) -> None:
        """Guard: raises RuntimeError if dataset hasn't been loaded yet."""
        if self.df is None:
            raise RuntimeError("Dataset not loaded. Call load() before running checks.")

    def _require_columns(self, *columns: str) -> None:
        """
        Guard: raises KeyError if any required column is missing from the DataFrame.
        Called at the start of checks that depend on specific columns.
        """
        missing = [col for col in columns if col not in self.df.columns]
        if missing:
            raise KeyError(
                f"Required column(s) missing from dataset: {missing}. "
                f"Run check_schema() first to diagnose schema issues."
            )

    def _build_error_result(self, check_name: str, error: Exception) -> dict:
        """
        Returns a standardised error result dict when a check fails to run.
        This allows run_all() to continue past a broken check rather than crash.
        """
        return _build_result(
            check_name=check_name,
            passed=False,
            summary=f"Check could not complete due to an error: {type(error).__name__}",
            details={"error_type": type(error).__name__, "error_message": str(error)},
        )

# Null Check
    def check_nulls(self) -> dict:
        """
        Detects missing (NaN / None) values across all columns.

        Returns:
            result dict with per-column null counts and total affected rows.
        """
        self._require_loaded()

        try:
            null_counts = self.df.isnull().sum()
            per_column = {col: int(count) for col, count in null_counts.items() if count > 0}
            total_nulls = int(null_counts.sum())
            affected_idx = list(self.df[self.df.isnull().any(axis=1)].index)

            passed = total_nulls == 0
            summary = (
                "No null values found." if passed
                else f"{total_nulls} null value(s) across {len(per_column)} column(s)."
            )

            result = _build_result(
                check_name="null_check",
                passed=passed,
                summary=summary,
                details={
                    "total_nulls":    total_nulls,
                    "null_by_column": per_column,
                    "affected_count": len(affected_idx),
                },
                affected_rows=affected_idx,
            )
        except Exception as e:
            result = self._build_error_result("null_check", e)
        
        self._results.append(result)
        return result

# Duplicate Check

    def check_duplicates(self) -> dict:
        """
        Detects fully duplicated rows and duplicate order_id values.

        Returns:
            result dict with counts and index positions of all duplicate rows.
        """
        self._require_loaded()
        try:
            full_dupes = self.df[self.df.duplicated(keep="first")]
            id_dupes = self.df[self.df.duplicated(subset=["order_id"], keep="first")]

            total = len(full_dupes)
            passed = total == 0 and len(id_dupes) == 0
            summary = (
                "No duplicate rows found." if passed
                else f"{total} fully duplicate row(s); {len(id_dupes)} duplicate order_id(s)."
            )

            result = _build_result(
                check_name="duplicate_check",
                passed=passed,
                summary=summary,
                details={
                    "full_duplicate_count":     total,
                    "duplicate_order_id_count": len(id_dupes),
                    "duplicate_order_ids":      list(id_dupes["order_id"].unique()),
                },
                affected_rows=list(full_dupes.index),
            )
        except Exception as e:
            result = self._build_error_result("duplicate_check", e)
            
        self._results.append(result)
        return result

# Outlier Check

    def check_outliers(self) -> dict:
        """
        Detects outliers in numeric columns using:
          - Business rules: quantity <= 0 or > 10,000 ; price <= 0
          - Statistical: values beyond 3 standard deviations from mean

        Returns:
            result dict with per-column outlier breakdown and IQR stats.
        """
        self._require_loaded()

        try: 
            self._require_columns("quantity", "unit_price_eur")
            findings = {}
            all_affected = set()

            for col in ["quantity", "unit_price_eur"]:
                series = self.df[col].dropna()
                
                if series.empty:
                    findings[col] = {"error": "Column is entirely null — cannot compute stats."}
                    continue

                # Business rule violations
                if col == "quantity":
                    rule_mask = (self.df[col] <= 0) | (self.df[col] > 10_000)
                else:
                    rule_mask = self.df[col] <= 0

                rule_idx = list(self.df[rule_mask].index)

                # Statistical outliers (Z-score > 3)
                mean, std = series.mean(), series.std()
                if std == 0 or pd.isna(std):
                    z_idx = []
                else:
                    z_mask = ((self.df[col] - mean).abs() > 3 * std) & self.df[col].notna()
                    z_idx = list(self.df[z_mask].index)

                combined = list(set(rule_idx + z_idx))
                all_affected.update(combined)

                findings[col] = {
                    "business_rule_violations": len(rule_idx),
                    "statistical_outliers_z3":  len(z_idx),
                    "combined_affected":        len(combined),
                    "mean":                     round(float(mean), 2),
                    "std":                      round(float(std), 2),
                    "min":                      round(float(series.min()), 2),
                    "max":                      round(float(series.max()), 2),
                }

            total_affected = len(all_affected)
            passed = total_affected == 0
            summary = (
                "No outliers detected." if passed
                else f"{total_affected} row(s) contain outlier values in numeric columns."
            )

            result = _build_result(
                check_name="outlier_check",
                passed=passed,
                summary=summary,
                details={"per_column": findings, "total_affected_rows": total_affected},
                affected_rows=sorted(all_affected),
            )
        except Exception as e:
            result = self._build_error_result("outlier_check", e)
            
        self._results.append(result)
        return result

# Schema Check

    def check_schema(self) -> dict:
        """
        Validates that all expected columns are present.
        Reports any missing or unexpected extra columns.

        Returns:
            result dict with missing columns, extra columns, and column count.
        """
        self._require_loaded()
        try:
            expected = set(EXPECTED_SCHEMA.keys())
            actual = set(self.df.columns)

            missing = sorted(expected - actual)
            extra = sorted(actual - expected)

            passed = len(missing) == 0
            summary = (
                "Schema is valid — all expected columns present." if passed
                else f"Schema mismatch: {len(missing)} missing column(s)."
            )

            result = _build_result(
                check_name="schema_check",
                passed=passed,
                summary=summary,
                details={
                    "expected_columns": sorted(expected),
                    "actual_columns":   sorted(actual),
                    "missing_columns":  missing,
                    "extra_columns":    extra,
                    "column_count":     {"expected": len(expected), "actual": len(actual)},
                },
            )
        except Exception as e:
            result = self._build_error_result("schema_check", e)
            
        self._results.append(result)
        return result

# Invalid Status Check

    def check_invalid_statuses(self) -> dict:
        """
        Validates that all 'status' values belong to the allowed set.

        Returns:
            result dict with invalid values found and their frequencies.
        """
        self._require_loaded()

        try:
            self._require_columns("status")
            invalid_mask = ~self.df["status"].isin(VALID_STATUSES) & self.df["status"].notna()
            invalid_rows = self.df[invalid_mask]
            value_counts = invalid_rows["status"].value_counts().to_dict()

            passed = len(invalid_rows) == 0
            summary = (
                "All status values are valid." if passed
                else f"{len(invalid_rows)} row(s) have invalid status values."
            )

            result = _build_result(
                check_name="status_check",
                passed=passed,
                summary=summary,
                details={
                    "valid_statuses":          sorted(VALID_STATUSES),
                    "invalid_values_found":    value_counts,
                    "total_invalid_rows":      len(invalid_rows),
                },
                affected_rows=list(invalid_rows.index),
            )
        except Exception as e:
            result = self._build_error_result("status_check", e)
            
        self._results.append(result)
        return result

# Date Format (Consistency) Check
    def check_date_formats(self) -> dict:
        """
        Checks order_date and delivery_date columns for:
          - Unparseable / corrupt date strings
          - Inconsistent formats across rows
          - Delivery date before order date (logical violation)

        Returns:
            result dict with format distribution and logical violations.
        """
        self._require_loaded()
        try:
            self._require_columns("order_date", "delivery_date")
            findings = {}
            all_affected = set()

            for col in ["order_date", "delivery_date"]:
                unparseable = []
                format_hits: dict[str, int] = {}

                for idx, val in self.df[col].items():
                    if pd.isna(val):
                        continue
                    parsed = False
                    for fmt in VALID_DATE_FORMATS:
                        try:
                            datetime.strptime(str(val), fmt)
                            format_hits[fmt] = format_hits.get(fmt, 0) + 1
                            parsed = True
                            break
                        except ValueError:
                            continue
                    if not parsed:
                        unparseable.append(idx)
                        all_affected.add(idx)

                findings[col] = {
                    "unparseable_count":  len(unparseable),
                    "format_distribution": format_hits,
                    "is_consistent":      len(format_hits) <= 1,
                }

            #Logic check: delivery_date must be >= order_date (ISO rows only)
            logical_violations = []
            for idx, row in self.df.iterrows():
                try:
                    order = datetime.strptime(str(row["order_date"]), "%Y-%m-%d")
                    delivery = datetime.strptime(str(row["delivery_date"]), "%Y-%m-%d")
                    if delivery < order:
                        logical_violations.append(idx)
                        all_affected.add(idx)
                except (ValueError, TypeError):
                    pass

            total_affected = len(all_affected)
            passed = total_affected == 0
            summary = (
                "All dates are valid and consistent." if passed
                else f"{total_affected} row(s) have date issues (format or logic)."
            )

            result = _build_result(
                check_name="date_format_check",
                passed=passed,
                summary=summary,
                details={
                    "per_column":                findings,
                    "logical_violations_count":  len(logical_violations),
                    "total_affected_rows":       total_affected,
                },
                affected_rows=sorted(all_affected),
            )
        except Exception as e:
            result = self._build_error_result("date_format_check", e)
            
        self._results.append(result)
        return result

# Negative Price Check

    def check_negative_prices(self) -> dict:
        """
        Flags rows where unit_price_eur is negative or zero — a business rule
        violation meaning no order should have a non-positive price.

        Returns:
            result dict with count and stats of offending rows.
        """
        self._require_loaded()
        try:
            mask = self.df["unit_price_eur"] <= 0
            bad_rows = self.df[mask & self.df["unit_price_eur"].notna()]

            passed = len(bad_rows) == 0
            summary = (
                "All unit prices are positive." if passed
                else f"{len(bad_rows)} row(s) have non-positive unit prices."
            )

            result = _build_result(
                check_name="negative_price_check",
                passed=passed,
                summary=summary,
                details={
                    "affected_count": len(bad_rows),
                    "min_price_found": round(float(bad_rows["unit_price_eur"].min()), 2) if len(bad_rows) > 0 else None,
                    "max_price_found": round(float(bad_rows["unit_price_eur"].max()), 2) if len(bad_rows) > 0 else None,
                },
                affected_rows=list(bad_rows.index),
            )
        except Exception as e:
            result = self._build_error_result("negatve_price_check", e)
            
        self._results.append(result)
        return result

# Run All The 
    def run_all(self) -> list[dict]:
        """
        Runs all quality checks in sequence.

        Returns:
            list of result dicts from every check.
        """
        self._require_loaded()
        self._results = []

        checks = [
            self.check_schema,
            self.check_nulls,
            self.check_duplicates,
            self.check_outliers,
            self.check_invalid_statuses,
            self.check_date_formats,
            self.check_negative_prices,
        ]

        print("Running all quality checks...\n")
        for check_fn in checks:
            try:
                result = check_fn()
            except Exception as e:
                # Last-resort catch — check method itself raised outside its own try/except
                status = "PASS" if result["passed"] else "FAIL"
                print(f"  {status}  [{result['check']}] — {result['summary']}")
            status = "PASS" if result["passed"] else "FAIL"
            print(f" [{status}]  {result['check']} - {result['summary']}")

        passed_count = sum(1 for r in self._results if r["passed"])
        print(f"Results: {passed_count}/{len(self._results)} checks passed.")
        return self._results

# Accessors
    def get_results(self) -> list[dict]:
        """Returns accumulated results from all checks run so far."""
        return self._results

    def get_failed_checks(self) -> list[dict]:
        """Returns only the checks that did not pass."""
        return [r for r in self._results if not r["passed"]]

    def get_summary_stats(self) -> dict:
        """
        Returns a high-level summary of the overall dataset health.

        Returns:
            dict with total rows, checks run, pass/fail counts, and health score.
        """
        self._require_loaded()
        total_checks = len(self._results)
        passed = sum(1 for r in self._results if r["passed"])
        health_score = round((passed / total_checks) * 100, 1) if total_checks > 0 else 0.0

        return {
            "filepath":      self.filepath,
            "total_rows":    len(self.df),
            "total_columns": len(self.df.columns),
            "checks_run":    total_checks,
            "checks_passed": passed,
            "checks_failed": total_checks - passed,
            "health_score":  f"{health_score}%",
        }