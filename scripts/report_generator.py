import os
from datetime import datetime
from typing import Any


# Formatting Helpers 

def _divider(char: str = "-", width: int = 60) -> str:
    return char * width

def _header(title: str, char: str = "=", width: int = 60) -> str:
    return f"\n{_divider(char, width)}\n{title.upper()}\n{_divider(char, width)}"

def _indent(text: str, spaces: int = 4) -> str:
    pad = " " * spaces
    return "\n".join(pad + line for line in text.splitlines())

def _format_value(val: Any) -> str:
    if val is None:
        return "N/A"
    if isinstance(val, bool):
        return "Yes" if val else "No"
    if isinstance(val, float):
        return f"{val:.2f}"
    return str(val)


# Section Renderers

def _render_overview(summary: dict) -> str:
    lines = [
        _header("Dataset Overview"),
        f"  File             : {summary.get('filepath', 'N/A')}",
        f"  Total Rows       : {summary.get('total_rows', 'N/A')}",
        f"  Total Columns    : {summary.get('total_columns', 'N/A')}",
        f"  Checks Run       : {summary.get('checks_run', 'N/A')}",
        f"  Checks Passed    : {summary.get('checks_passed', 'N/A')}",
        f"  Checks Failed    : {summary.get('checks_failed', 'N/A')}",
        f"  Health Score     : {summary.get('health_score', 'N/A')}",
    ]
    return "\n".join(lines)


def _render_check_result(result: dict, index: int) -> str:
    status = "PASS" if result["passed"] else "FAIL"
    lines = [
        f"\n{_divider('-', 60)}",
        f"Check {index:02d} | {result['check'].upper().replace('_', ' ')} | {status}",
        _divider("-", 60),
        f"  Summary   : {result['summary']}",
        f"  Timestamp : {result['timestamp']}",
        f"  Affected  : {len(result['affected_rows'])} row(s)",
        "",
        "  Details:",
    ]

    details = result.get("details", {})
    lines += _render_details(details)

    if result["affected_rows"] and len(result["affected_rows"]) <= 20:
        lines.append(f"\n  Affected Row Indices:")
        lines.append(f"    {result['affected_rows']}")
    elif result["affected_rows"]:
        preview = result["affected_rows"][:20]
        lines.append(f"\n  Affected Row Indices (first 20 of {len(result['affected_rows'])}):")
        lines.append(f"    {preview}")

    return "\n".join(lines)


def _render_details(details: dict, depth: int = 2) -> list[str]:
    """Recursively renders a nested details dict into indented lines."""
    lines = []
    pad = "  " * depth
    for key, value in details.items():
        label = key.replace("_", " ").capitalize()
        if isinstance(value, dict):
            lines.append(f"{pad}{label}:")
            lines += _render_details(value, depth + 1)
        elif isinstance(value, list):
            if len(value) == 0:
                lines.append(f"{pad}{label}: (none)")
            elif len(value) <= 10:
                lines.append(f"{pad}{label}: {value}")
            else:
                lines.append(f"{pad}{label}: {value[:10]} ... ({len(value)} total)")
        else:
            lines.append(f"{pad}{label}: {_format_value(value)}")
    return lines


def _render_failed_summary(results: list[dict]) -> str:
    failed = [r for r in results if not r["passed"]]
    lines = [_header("Failed Checks Summary")]

    if not failed:
        lines.append("  All checks passed. No issues to report.")
        return "\n".join(lines)

    for r in failed:
        lines.append(f"\n  [{r['check']}]")
        lines.append(f"    {r['summary']}")
        lines.append(f"    Affected rows: {len(r['affected_rows'])}")

    return "\n".join(lines)


def _render_recommendations(results: list[dict]) -> str:
    failed = [r for r in results if not r["passed"]]
    lines = [_header("Recommendations")]

    if not failed:
        lines.append("  No corrective action required.")
        return "\n".join(lines)

    recommendations = {
        "null_check": (
            "Investigate upstream data sources for missing fields. "
            "Consider adding NOT NULL constraints at ingestion layer."
        ),
        "duplicate_check": (
            "Deduplicate records using order_id as the primary key. "
            "Add unique key enforcement in the pipeline."
        ),
        "outlier_check": (
            "Review quantity and price bounds with business stakeholders. "
            "Add min/max validation rules at data entry points."
        ),
        "schema_check": (
            "Align source schema with expected contract. "
            "Introduce schema validation at pipeline entry."
        ),
        "status_check": (
            "Standardise status values to the approved enum set. "
            "Reject or quarantine records with unknown statuses."
        ),
        "date_format_check": (
            "Enforce ISO 8601 (YYYY-MM-DD) as the single accepted date format. "
            "Add pre-processing normalisation for legacy format inputs."
        ),
        "negative_price_check": (
            "Reject records with non-positive unit prices at ingestion. "
            "Flag for manual review — may indicate refunds or data entry errors."
        ),
    }

    for r in failed:
        rec = recommendations.get(r["check"], "Review the affected rows and investigate the root cause.")
        lines.append(f"\n  [{r['check']}]")
        lines.append(f"    {rec}")

    return "\n".join(lines)


# ReportGenerator Class
class ReportGenerator:
    """
    Generates a structured plain-text quality report from DataQualityChecker results.

    Attributes:
        results       (list[dict]) : list of result dicts from DataQualityChecker
        summary_stats (dict)       : overall stats from get_summary_stats()
        _report_lines (list[str])  : internal buffer of rendered report lines
    """

    def __init__(self, results: list[dict], summary_stats: dict):
        self.results = results
        self.summary_stats = summary_stats
        self._report_lines: list[str] = []

    def _build(self) -> None:
        """Assembles all sections into the internal line buffer."""
        self._report_lines = []

        # Title block
        generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        self._report_lines += [
            _divider("=", 60),
            "SCM DATA QUALITY REPORT",
            f"Generated : {generated_at}",
            _divider("=", 60),
        ]

        # Overview
        self._report_lines.append(_render_overview(self.summary_stats))

        # Individual check results
        self._report_lines.append(_header("Check Results"))
        for i, result in enumerate(self.results, start=1):
            self._report_lines.append(_render_check_result(result, i))

        # Failed summary
        self._report_lines.append(_render_failed_summary(self.results))

        # Recommendations
        self._report_lines.append(_render_recommendations(self.results))

        # Footer
        self._report_lines += [
            f"\n{_divider('=', 60)}",
            "END OF REPORT",
            _divider("=", 60),
        ]

    def write(self, output_path: str) -> None:
        """
        Builds and writes the report to the specified file path.

        Args:
            output_path (str): destination file path, e.g. 'reports/quality_report.txt'
        """
        self._build()
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        report_text = "\n".join(self._report_lines)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report_text)

        print(f"Report written to: {output_path}")

    def to_string(self) -> str:
        """Returns the full report as a string without writing to disk."""
        self._build()
        return "\n".join(self._report_lines)