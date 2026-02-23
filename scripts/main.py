"""
main.py
-------
Entry point for the SCM Data Quality pipeline.

Steps:
    1. Generate the synthetic dataset (if it doesn't exist)
    2. Load and run all quality checks
    3. Write the quality report to disk
    4. Print a final summary to stdout

Usage:
    python main.py
    python main.py --skip-generate   # use existing CSV
    python main.py --report-path reports/custom_report.txt
"""

import argparse
import sys
import os

from generate_dataset import main as generate_dataset
from quality_checker import DataQualityChecker
from report_generator import ReportGenerator

# Defaults 

DEFAULT_DATA_PATH   = "data/scm_orders.csv"
DEFAULT_REPORT_PATH = "reports/quality_report.txt"


# CLI Arguments

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SCM Data Quality Checker — automated QA pipeline for supply chain datasets."
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Skip dataset generation and use existing CSV file.",
    )
    parser.add_argument(
        "--data-path",
        type=str,
        default=DEFAULT_DATA_PATH,
        help=f"Path to the input CSV file. Default: {DEFAULT_DATA_PATH}",
    )
    parser.add_argument(
        "--report-path",
        type=str,
        default=DEFAULT_REPORT_PATH,
        help=f"Output path for the quality report. Default: {DEFAULT_REPORT_PATH}",
    )
    return parser.parse_args()


# Pipeline Steps

def step_generate(data_path: str, skip: bool) -> None:
    if skip:
        if not os.path.exists(data_path):
            print(f"[ERROR] --skip-generate was set but no file found at '{data_path}'.")
            sys.exit(1)
        print(f"[STEP 1] Skipping dataset generation. Using: {data_path}")
    else:
        print("[STEP 1] Generating synthetic SCM dataset...")
        generate_dataset()


def step_check(data_path: str) -> tuple[list[dict], dict]:
    print("\n[STEP 2] Running data quality checks...")
    checker = DataQualityChecker(data_path)
    checker.load()
    results = checker.run_all()
    summary = checker.get_summary_stats()
    return results, summary


def step_report(results: list[dict], summary: dict, report_path: str) -> None:
    print(f"\n[STEP 3] Writing quality report...")
    generator = ReportGenerator(results, summary)
    generator.write(report_path)


def step_print_summary(summary: dict, results: list[dict]) -> None:
    failed = [r for r in results if not r["passed"]]
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Dataset          : {summary['filepath']}")
    print(f"  Rows analysed    : {summary['total_rows']}")
    print(f"  Checks passed    : {summary['checks_passed']} / {summary['checks_run']}")
    print(f"  Health score     : {summary['health_score']}")

    if failed:
        print(f"\n  Failed checks ({len(failed)}):")
        for r in failed:
            print(f"    - [{r['check']}] {r['summary']}")
    else:
        print("\n  All checks passed.")

    print("=" * 60)


# Main

def main() -> None:
    args = parse_args()

    step_generate(args.data_path, args.skip_generate)
    results, summary = step_check(args.data_path)
    step_report(results, summary, args.report_path)
    step_print_summary(summary, results)


if __name__ == "__main__":
    main()