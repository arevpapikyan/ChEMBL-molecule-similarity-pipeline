#!/usr/bin/env python3

import argparse
import json
import os
import sys
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

BRONZE_TABLES = (
    "chembl_id_lookup",
    "molecule_dictionary",
    "compound_properties",
    "compound_structures",
)

# view name -> how many rows to show in the sample
REPORTING_VIEWS = {
    "v7a_avg_similarity_per_source": 5,
    "v7b_avg_alogp_deviation": 5,
    "v8a_similarity_pivot": 5,
    "v8b_next_and_second_target": 5,
    "v8c_avg_similarity_grouped": 12,
}

PIVOT_VIEW = "v8a_similarity_pivot"
PIVOT_SOURCE_COLUMNS = 10

EXIT_SUCCESS = 0
EXIT_CHECKS_FAILED = 1
EXIT_CANNOT_VERIFY = 2

RULE = "=" * 72


def _bootstrap_pipeline_package() -> None:
    """Put the `pipeline` package on sys.path."""
    candidates = (
        Path("/app"),
        Path(__file__).resolve().parent.parent / "workers",
    )
    for candidate in candidates:
        if (candidate / "pipeline" / "config.py").is_file():
            sys.path.insert(0, str(candidate))
            return
    raise SystemExit(
        "Could not find the 'pipeline' package. Looked in: "
        + ", ".join(str(c) for c in candidates)
    )


_bootstrap_pipeline_package()

# These imports deliberately follow the sys.path bootstrap above; they cannot be
# hoisted to the top of the file because the package is not importable until it
# has run.
from pipeline.config import get_settings  # noqa: E402
from pipeline.s3_utils import list_keys, read_parquet  # noqa: E402

# ------ Result collection ------

@dataclass(frozen=True)
class CheckResult:
    """One named assertion about the state of a completed run."""

    name: str
    passed: bool
    detail: str = ""

    def format_line(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        suffix = f" -- {self.detail}" if self.detail else ""
        return f"  [{status}] {self.name}{suffix}"


@dataclass
class VerificationReport:
    """Accumulates check results and the measured facts behind them."""

    results: list[CheckResult] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def record(self, name: str, passed: bool, detail: str = "") -> CheckResult:
        result = CheckResult(name=name, passed=bool(passed), detail=detail)
        self.results.append(result)
        print(result.format_line())
        return result

    def measure(self, name: str, value: Any) -> Any:
        self.metrics[name] = value
        return value

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    @property
    def passed(self) -> bool:
        return not self.failures

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "checks_run": len(self.results),
            "checks_failed": len(self.failures),
            "metrics": self.metrics,
            "results": [
                {"name": r.name, "passed": r.passed, "detail": r.detail}
                for r in self.results
            ],
        }


def render_table(rows: list[dict[str, Any]], limit: int = 10) -> None:
    """Print a list of dicts as an aligned markdown-style table."""
    visible = list(rows)[:limit]
    if not visible:
        print("    (no rows)")
        return

    def cell(value: Any) -> str:
        # dim_molecule's cx_logp and molecular_species are always NULL by
        # design; rendering them as Python's "None" reads like a bug.
        return "NULL" if value is None else str(value)

    columns = list(visible[0].keys())
    widths = [
        max(len(column), *(len(cell(row[column])) for row in visible))
        for column in columns
    ]
    header = " | ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True))
    print(f"    | {header} |")
    print("    |-" + "-|-".join("-" * w for w in widths) + "-|")
    for row in visible:
        line = " | ".join(cell(row[c]).ljust(w) for c, w in zip(columns, widths, strict=True))
        print(f"    | {line} |")


def _scalar(cursor, query, params: tuple = ()) -> Any:
    """Run a query expected to yield a single column named `n` and return it."""
    cursor.execute(query, params)
    return cursor.fetchone()["n"]


# ------ Layer checks ------

def verify_bronze(cursor, report: VerificationReport) -> None:
    print(RULE)
    print("1. BRONZE (raw schema)")
    print(RULE)
    for table_name in BRONZE_TABLES:
        row_count = _scalar(
            cursor,
            sql.SQL("SELECT count(*) AS n FROM raw.{}").format(sql.Identifier(table_name)),
        )
        report.measure(f"bronze.{table_name}", row_count)
        print(f"  raw.{table_name:<22} {row_count:>10,} rows")


def verify_silver(settings, report: VerificationReport, expected_sources: int) -> None:
    print()
    print(RULE)
    print("2. SILVER (S3)")
    print(RULE)

    top_k_prefix = f"{settings.s3_prefix}/silver/topk/"
    prefixes = {
        "fingerprints": f"{settings.silver_fingerprints_prefix}/",
        "similarity": f"{settings.silver_similarity_prefix}/",
        "topk": top_k_prefix,
    }
    for label, prefix in prefixes.items():
        object_count = len(list_keys(settings, prefix))
        report.measure(f"silver.{label}_objects", object_count)
        print(f"  {label:<14} {object_count:>5} objects")

    top_k_keys = [k for k in list_keys(settings, top_k_prefix) if k.endswith("top10.parquet")]
    report.measure("silver.topk_files", len(top_k_keys))
    report.record(
        "exactly one top-k file per source",
        len(top_k_keys) == expected_sources,
        f"found {len(top_k_keys)}, expected {expected_sources}",
    )

    if top_k_keys:
        print("\n  sample of one top-k parquet:")
        sample = read_parquet(settings, sorted(top_k_keys)[0])
        render_table(sample.to_pylist(), limit=10)


def verify_gold(cursor, report: VerificationReport, expected_sources: int, top_k: int) -> None:
    print()
    print(RULE)
    print("3. GOLD (mart schema)")
    print(RULE)

    dim_rows = report.measure("gold.dim_molecule_rows", _scalar(
        cursor, "SELECT count(*) AS n FROM mart.dim_molecule"))
    fact_rows = report.measure("gold.fact_similarity_rows", _scalar(
        cursor, "SELECT count(*) AS n FROM mart.fact_similarity"))
    distinct_sources = report.measure("gold.distinct_sources", _scalar(
        cursor, "SELECT count(DISTINCT source_chembl_id) AS n FROM mart.fact_similarity"))

    print(f"  mart.dim_molecule      {dim_rows:>10,} rows")
    print(f"  mart.fact_similarity   {fact_rows:>10,} rows  ({distinct_sources} distinct sources)")

    expected_fact_rows = expected_sources * top_k
    report.record(
        "source molecule count",
        distinct_sources == expected_sources,
        f"{distinct_sources} distinct sources, expected {expected_sources}",
    )
    report.record(
        f"top-{top_k} per source",
        fact_rows == expected_fact_rows,
        f"{fact_rows} fact rows, expected {expected_fact_rows}",
    )

    mismatched = _scalar(cursor, """
        SELECT count(*) AS n FROM (
            SELECT source_chembl_id FROM mart.fact_similarity
            GROUP BY source_chembl_id HAVING count(*) <> %s) AS offenders
    """, (top_k,))
    report.record(f"every source has exactly {top_k} targets", mismatched == 0,
                  "" if mismatched == 0 else f"{mismatched} sources with a different count")

    self_similar = _scalar(cursor, """
        SELECT count(*) AS n FROM mart.fact_similarity
        WHERE source_chembl_id = target_chembl_id
    """)
    report.record("no self-similarity rows", self_similar == 0,
                  "" if self_similar == 0 else f"{self_similar} rows")

    unreferenced = _scalar(cursor, """
        SELECT count(*) AS n FROM mart.dim_molecule d
        WHERE NOT EXISTS (SELECT 1 FROM mart.fact_similarity f
                          WHERE f.source_chembl_id = d.chembl_id
                             OR f.target_chembl_id = d.chembl_id)
    """)
    report.record("dim_molecule holds only referenced molecules", unreferenced == 0,
                  "" if unreferenced == 0 else f"{unreferenced} unreferenced rows")

    duplicate_ranks = _scalar(cursor, """
        SELECT count(*) AS n FROM (
            SELECT source_chembl_id FROM mart.fact_similarity
            GROUP BY source_chembl_id
            HAVING count(DISTINCT rank_within_source) <> count(*)) AS offenders
    """)
    report.record("ranks unique within each source", duplicate_ranks == 0,
                  "" if duplicate_ranks == 0 else f"{duplicate_ranks} sources with repeated ranks")

    # Reported, not asserted: how many boundary ties occur is data-dependent.
    flagged = report.measure("gold.tie_flagged_rows", _scalar(cursor, """
        SELECT count(*) AS n FROM mart.fact_similarity
        WHERE has_duplicates_of_last_largest_score
    """))
    print(f"    (rows flagged has_duplicates_of_last_largest_score: {flagged})")

    print("\n  sample of mart.fact_similarity (one source):")
    cursor.execute("""
        SELECT source_chembl_id, target_chembl_id, similarity_score,
               rank_within_source, has_duplicates_of_last_largest_score
        FROM mart.fact_similarity
        WHERE source_chembl_id = (SELECT min(source_chembl_id) FROM mart.fact_similarity)
        ORDER BY rank_within_source
    """)
    render_table(cursor.fetchall(), limit=top_k)

    print("\n  sample of mart.dim_molecule:")
    cursor.execute("""
        SELECT chembl_id, molecule_type, mw_freebase, alogp, psa,
               cx_logp, molecular_species, aromatic_rings, heavy_atoms
        FROM mart.dim_molecule ORDER BY chembl_id LIMIT 5
    """)
    render_table(cursor.fetchall(), limit=5)


def verify_views(cursor, report: VerificationReport) -> None:
    print()
    print(RULE)
    print("4. VIEWS")
    print(RULE)
    for view_name, sample_limit in REPORTING_VIEWS.items():
        print(f"\n  mart.{view_name}")
        qualified = sql.SQL("mart.{}").format(sql.Identifier(view_name))
        try:
            row_count = _scalar(cursor, sql.SQL("SELECT count(*) AS n FROM {}").format(qualified))
            cursor.execute(
                sql.SQL("SELECT * FROM {} LIMIT %s").format(qualified), (sample_limit,))
            rows = cursor.fetchall()
        except psycopg2.Error as exc:
            # The connection is in autocommit, so there is no aborted
            # transaction to roll back before moving on to the next view.
            report.record(f"{view_name} queryable", False,
                          str(exc).strip().splitlines()[0])
            continue

        report.measure(f"views.{view_name}_rows", row_count)
        report.record(f"{view_name} returns rows", row_count > 0, f"{row_count} rows")

        if view_name == PIVOT_VIEW and rows:
            source_columns = len(rows[0]) - 1  # minus the target_chembl_id key column
            report.record(
                f"pivot has {PIVOT_SOURCE_COLUMNS} source columns",
                source_columns == PIVOT_SOURCE_COLUMNS,
                f"{source_columns} columns",
            )
        render_table(rows, limit=sample_limit)


# ------ Entry point ------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify a completed ChEMBL similarity pipeline run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--expected-sources", type=int,
        default=int(os.environ.get("N_SOURCE_MOLECULES", "100")),
        help="Number of source molecules the run should have produced.",
    )
    parser.add_argument(
        "--top-k", type=int, default=int(os.environ.get("TOP_K", "10")),
        help="Neighbours expected per source molecule.",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Also emit a machine-readable summary on the last line of stdout.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        settings = get_settings()
    except (RuntimeError, ValueError) as exc:
        print(f"Cannot verify: {exc}", file=sys.stderr)
        return EXIT_CANNOT_VERIFY

    report = VerificationReport()
    try:
        with closing(psycopg2.connect(settings.dwh_url, connect_timeout=30)) as connection:
            connection.autocommit = True
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                verify_bronze(cursor, report)
                verify_silver(settings, report, args.expected_sources)
                verify_gold(cursor, report, args.expected_sources, args.top_k)
                verify_views(cursor, report)
    except psycopg2.Error as exc:
        print(f"Cannot verify: database error: {exc}", file=sys.stderr)
        return EXIT_CANNOT_VERIFY

    print()
    print(RULE)
    if report.passed:
        print(f"OVERALL: ALL CHECKS PASSED ({len(report.results)} checks)")
    else:
        print(f"OVERALL: {len(report.failures)} OF {len(report.results)} CHECKS FAILED")
        for failure in report.failures:
            detail = f" -- {failure.detail}" if failure.detail else ""
            print(f"  - {failure.name}{detail}")
    print(RULE)

    if args.json:
        print(json.dumps(report.as_dict(), default=str))

    return EXIT_SUCCESS if report.passed else EXIT_CHECKS_FAILED


if __name__ == "__main__":
    sys.exit(main())
