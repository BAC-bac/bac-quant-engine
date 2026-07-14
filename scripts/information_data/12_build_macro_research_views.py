from __future__ import annotations

import platform
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

REPORT_DIR = PROJECT_ROOT / "reports"
REPORT_CSV = REPORT_DIR / "macro_research_views_catalogue_latest.csv"
REPORT_TXT = REPORT_DIR / "macro_research_views_latest.txt"


VIEW_DEFINITIONS = [
    {
        "view_name": "latest_fred_series_values",
        "source_table": "fred_macro_series",
        "description": (
            "Most recent non-null observation for each FRED series."
        ),
        "required_columns": [
            "series_id",
            "series_name",
            "date",
            "value",
        ],
        "date_column": "date",
        "sql_builder": "fred_latest",
    },
    {
        "view_name": "latest_boe_bank_rate",
        "source_table": "boe_bank_rate",
        "description": (
            "Latest official Bank of England Bank Rate observation."
        ),
        "required_columns": [
            "date",
            "bank_rate_percent",
        ],
        "date_column": "date",
        "sql_builder": "latest_single_date",
    },
    {
        "view_name": "latest_us_treasury_average_rates",
        "source_table": "us_treasury_average_interest_rates",
        "description": (
            "All US Treasury average-interest-rate records from "
            "the latest available reporting date."
        ),
        "required_columns": [
            "record_date",
            "security_type_desc",
            "security_desc",
            "avg_interest_rate_amt",
        ],
        "date_column": "record_date",
        "sql_builder": "latest_single_date",
    },
    {
        "view_name": "latest_cftc_positioning",
        "source_table": "cftc_cot_tff",
        "description": (
            "Latest available CFTC TFF positioning records for "
            "each contract or market."
        ),
        "required_columns": [
            "report_date_as_yyyy_mm_dd",
            "market_and_exchange_names",
        ],
        "date_column": "report_date_as_yyyy_mm_dd",
        "sql_builder": "cftc_latest_by_contract",
    },
    {
        "view_name": "latest_cross_asset_snapshot",
        "source_table": "cross_asset_macro_snapshots",
        "description": (
            "Most recent complete cross-asset market snapshot."
        ),
        "required_columns": [
            "snapshot_date",
        ],
        "date_column": "snapshot_date",
        "sql_builder": "latest_single_date",
    },
    {
        "view_name": "latest_financial_headlines",
        "source_table": "financial_headline_snapshots",
        "description": (
            "Financial headlines from the most recently collected "
            "RSS snapshot."
        ),
        "required_columns": [
            "snapshot_date",
            "feed_name",
            "headline_clean",
        ],
        "date_column": "snapshot_date",
        "sql_builder": "latest_single_date",
    },
    {
        "view_name": "latest_economic_calendar",
        "source_table": "economic_calendar_snapshots",
        "description": (
            "Latest economic-calendar snapshot, when the optional "
            "calendar source is available."
        ),
        "required_columns": [
            "snapshot_date",
        ],
        "date_column": "snapshot_date",
        "sql_builder": "latest_single_date",
        "optional": True,
    },
]


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"Could not find configuration file: {CONFIG_FILE}"
        )

    with CONFIG_FILE.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(
            f"Invalid YAML configuration: {CONFIG_FILE}"
        )

    return config


def select_existing_path(
    candidates: list[str | None],
) -> Path:
    valid_candidates = [
        candidate
        for candidate in candidates
        if candidate
    ]

    if not valid_candidates:
        raise ValueError(
            "No Data Lake path candidates were configured."
        )

    for candidate in valid_candidates:
        path = Path(candidate)

        if path.exists():
            return path

    raise FileNotFoundError(
        "None of the configured Data Lake paths exists: "
        + ", ".join(valid_candidates)
    )


def get_data_lake_root() -> Path:
    config = load_config()
    paths = config["data_lake_root"]

    if platform.system().lower() == "windows":
        return select_existing_path(
            [
                paths.get("windows_network"),
                paths.get("windows_local"),
                paths.get("windows"),
            ]
        )

    linux_path = paths.get("linux")

    if not linux_path:
        raise KeyError(
            "'data_lake_root.linux' is missing from config/paths.yaml"
        )

    resolved_path = Path(linux_path)

    if not resolved_path.exists():
        raise FileNotFoundError(
            f"Configured Linux Data Lake does not exist: {resolved_path}"
        )

    return resolved_path


def get_database_path(
    data_lake_root: Path,
) -> Path:
    return (
        data_lake_root
        / "data"
        / "curated"
        / "macro"
        / "master_database"
        / "macro_master_database_latest.sqlite"
    )


def quote_identifier(identifier: str) -> str:
    safe_identifier = identifier.replace('"', '""')
    return f'"{safe_identifier}"'


def object_exists(
    connection: sqlite3.Connection,
    object_name: str,
    object_type: str | None = None,
) -> bool:
    sql = """
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE name = ?
    """
    params: list[str] = [object_name]

    if object_type:
        sql += " AND type = ?"
        params.append(object_type)

    count = connection.execute(
        sql,
        params,
    ).fetchone()[0]

    return count > 0


def get_table_columns(
    connection: sqlite3.Connection,
    table_name: str,
) -> list[str]:
    if not object_exists(
        connection,
        table_name,
        "table",
    ):
        return []

    rows = connection.execute(
        f"PRAGMA table_info({quote_identifier(table_name)})"
    ).fetchall()

    return [row[1] for row in rows]


def build_latest_single_date_sql(
    source_table: str,
    date_column: str,
) -> str:
    quoted_table = quote_identifier(source_table)
    quoted_date = quote_identifier(date_column)

    return f"""
        SELECT *
        FROM {quoted_table}
        WHERE {quoted_date} = (
            SELECT MAX({quoted_date})
            FROM {quoted_table}
            WHERE {quoted_date} IS NOT NULL
        )
    """


def build_fred_latest_sql(
    source_table: str,
) -> str:
    quoted_table = quote_identifier(source_table)

    return f"""
        SELECT source_rows.*
        FROM {quoted_table} AS source_rows
        INNER JOIN (
            SELECT
                series_id,
                MAX(date) AS latest_date
            FROM {quoted_table}
            WHERE
                date IS NOT NULL
                AND value IS NOT NULL
            GROUP BY series_id
        ) AS latest_rows
            ON source_rows.series_id = latest_rows.series_id
           AND source_rows.date = latest_rows.latest_date
        WHERE source_rows.value IS NOT NULL
    """


def choose_cftc_group_columns(
    available_columns: list[str],
) -> list[str]:
    preferred_groups = [
        "cftc_contract_market_code",
        "market_and_exchange_names",
    ]

    selected = [
        column
        for column in preferred_groups
        if column in available_columns
    ]

    if selected:
        return selected

    fallback_groups = [
        "market_and_exchange_names",
        "cftc_market_code",
        "cftc_commodity_code",
    ]

    return [
        column
        for column in fallback_groups
        if column in available_columns
    ]


def build_cftc_latest_sql(
    source_table: str,
    date_column: str,
    available_columns: list[str],
) -> str:
    group_columns = choose_cftc_group_columns(
        available_columns
    )

    if not group_columns:
        return build_latest_single_date_sql(
            source_table,
            date_column,
        )

    quoted_table = quote_identifier(source_table)
    quoted_date = quote_identifier(date_column)

    group_clause = ", ".join(
        quote_identifier(column)
        for column in group_columns
    )

    join_conditions = "\n           AND ".join(
        (
            f"source_rows.{quote_identifier(column)} "
            f"IS latest_rows.{quote_identifier(column)}"
        )
        for column in group_columns
    )

    return f"""
        SELECT source_rows.*
        FROM {quoted_table} AS source_rows
        INNER JOIN (
            SELECT
                {group_clause},
                MAX({quoted_date}) AS latest_date
            FROM {quoted_table}
            WHERE {quoted_date} IS NOT NULL
            GROUP BY {group_clause}
        ) AS latest_rows
            ON {join_conditions}
           AND source_rows.{quoted_date} = latest_rows.latest_date
    """


def build_view_sql(
    definition: dict,
    available_columns: list[str],
) -> str:
    builder = definition["sql_builder"]
    source_table = definition["source_table"]
    date_column = definition["date_column"]

    if builder == "latest_single_date":
        return build_latest_single_date_sql(
            source_table,
            date_column,
        )

    if builder == "fred_latest":
        return build_fred_latest_sql(
            source_table
        )

    if builder == "cftc_latest_by_contract":
        return build_cftc_latest_sql(
            source_table=source_table,
            date_column=date_column,
            available_columns=available_columns,
        )

    raise ValueError(
        f"Unknown SQL builder: {builder}"
    )


def drop_existing_view(
    connection: sqlite3.Connection,
    view_name: str,
) -> None:
    connection.execute(
        f"DROP VIEW IF EXISTS "
        f"{quote_identifier(view_name)}"
    )


def create_view(
    connection: sqlite3.Connection,
    definition: dict,
) -> dict:
    view_name = definition["view_name"]
    source_table = definition["source_table"]
    optional = definition.get("optional", False)

    result = {
        "view_name": view_name,
        "source_table": source_table,
        "description": definition["description"],
        "optional": optional,
        "status": None,
        "row_count": None,
        "column_count": None,
        "date_column": definition["date_column"],
        "min_date": None,
        "max_date": None,
        "error": None,
    }

    if not object_exists(
        connection,
        source_table,
        "table",
    ):
        result["status"] = (
            "skipped_optional_source_missing"
            if optional
            else "failed_source_table_missing"
        )
        return result

    available_columns = get_table_columns(
        connection,
        source_table,
    )

    missing_columns = sorted(
        set(definition["required_columns"])
        - set(available_columns)
    )

    if missing_columns:
        result["status"] = (
            "skipped_optional_schema_mismatch"
            if optional
            else "failed_schema_mismatch"
        )
        result["error"] = (
            "Missing required columns: "
            f"{missing_columns}"
        )
        return result

    try:
        sql = build_view_sql(
            definition,
            available_columns,
        )

        drop_existing_view(
            connection,
            view_name,
        )

        connection.execute(
            f"""
                CREATE VIEW {quote_identifier(view_name)} AS
                {sql}
            """
        )

        row_count = connection.execute(
            f"""
                SELECT COUNT(*)
                FROM {quote_identifier(view_name)}
            """
        ).fetchone()[0]

        column_count = len(
            connection.execute(
                f"""
                    SELECT *
                    FROM {quote_identifier(view_name)}
                    LIMIT 0
                """
            ).description
        )

        date_column = definition["date_column"]

        min_date, max_date = connection.execute(
            f"""
                SELECT
                    MIN({quote_identifier(date_column)}),
                    MAX({quote_identifier(date_column)})
                FROM {quote_identifier(view_name)}
            """
        ).fetchone()

        result["status"] = "created"
        result["row_count"] = row_count
        result["column_count"] = column_count
        result["min_date"] = min_date
        result["max_date"] = max_date

    except Exception as exc:
        result["status"] = "failed_create_view"
        result["error"] = str(exc)

    return result


def create_view_catalogue(
    connection: sqlite3.Connection,
    results_df: pd.DataFrame,
    run_time_utc: datetime,
) -> None:
    catalogue = results_df.copy()
    catalogue["generated_utc"] = (
        run_time_utc.isoformat()
    )

    catalogue.to_sql(
        "research_view_catalogue",
        connection,
        if_exists="replace",
        index=False,
    )


def validate_created_view(
    connection: sqlite3.Connection,
    view_name: str,
) -> tuple[bool, str]:
    try:
        connection.execute(
            f"""
                SELECT *
                FROM {quote_identifier(view_name)}
                LIMIT 1
            """
        ).fetchall()

        return True, ""

    except Exception as exc:
        return False, str(exc)


def write_reports(
    results_df: pd.DataFrame,
    database_path: Path,
    run_time_utc: datetime,
) -> None:
    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    results_df.to_csv(
        REPORT_CSV,
        index=False,
    )

    created = results_df[
        results_df["status"] == "created"
    ]

    required_failures = results_df[
        ~results_df["optional"]
        & (
            results_df["status"]
            != "created"
        )
    ]

    optional_skips = results_df[
        results_df["optional"]
        & (
            results_df["status"]
            != "created"
        )
    ]

    lines = [
        "=" * 100,
        "BACQE MACRO RESEARCH VIEWS REPORT",
        "=" * 100,
        f"Generated UTC:       {run_time_utc.isoformat()}",
        f"Database:            {database_path}",
        "",
        f"Registered views:    {len(results_df)}",
        f"Views created:       {len(created)}",
        f"Required failures:   {len(required_failures)}",
        f"Optional skipped:    {len(optional_skips)}",
        "",
        "-" * 100,
        "RESEARCH VIEW CATALOGUE",
        "-" * 100,
    ]

    display_columns = [
        "view_name",
        "source_table",
        "optional",
        "status",
        "row_count",
        "column_count",
        "date_column",
        "min_date",
        "max_date",
    ]

    lines.append(
        results_df[
            display_columns
        ].to_string(index=False)
    )

    if not required_failures.empty:
        lines.extend(
            [
                "",
                "-" * 100,
                "REQUIRED VIEW FAILURES",
                "-" * 100,
                required_failures[
                    [
                        "view_name",
                        "source_table",
                        "status",
                        "error",
                    ]
                ].to_string(index=False),
            ]
        )

    if not optional_skips.empty:
        lines.extend(
            [
                "",
                "-" * 100,
                "OPTIONAL VIEWS SKIPPED",
                "-" * 100,
                optional_skips[
                    [
                        "view_name",
                        "source_table",
                        "status",
                        "error",
                    ]
                ].to_string(index=False),
            ]
        )

    lines.extend(
        [
            "",
            "=" * 100,
            "OVERALL RESULT",
            "=" * 100,
        ]
    )

    if required_failures.empty:
        lines.append(
            "PASS — all required research views were created."
        )
    else:
        lines.append(
            "FAILED — one or more required research views "
            "could not be created."
        )

    REPORT_TXT.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    print("=" * 100)
    print(
        "BACQE INFORMATION DATA 12 - "
        "BUILD MACRO RESEARCH VIEWS"
    )
    print("=" * 100)

    run_time_utc = datetime.now(
        timezone.utc
    )

    data_lake_root = get_data_lake_root()
    database_path = get_database_path(
        data_lake_root
    )

    if not database_path.exists():
        raise FileNotFoundError(
            "Macro Master Database not found: "
            f"{database_path}. Run INFO-11 first."
        )

    print(f"Data lake root: {data_lake_root}")
    print(f"Database:       {database_path}")
    print("-" * 100)

    results: list[dict] = []

    with sqlite3.connect(
        database_path
    ) as connection:
        for definition in VIEW_DEFINITIONS:
            print(
                f"[VIEW] {definition['view_name']:<42}",
                end="",
            )

            result = create_view(
                connection,
                definition,
            )

            results.append(result)

            print(f" {result['status']}")

        results_df = pd.DataFrame(
            results
        )

        created_views = results_df[
            results_df["status"] == "created"
        ]

        for view_name in created_views[
            "view_name"
        ]:
            valid, error = validate_created_view(
                connection,
                view_name,
            )

            if not valid:
                results_df.loc[
                    results_df["view_name"]
                    == view_name,
                    "status",
                ] = "failed_validation"

                results_df.loc[
                    results_df["view_name"]
                    == view_name,
                    "error",
                ] = error

        create_view_catalogue(
            connection=connection,
            results_df=results_df,
            run_time_utc=run_time_utc,
        )

        connection.execute(
            "PRAGMA optimize;"
        )

        connection.commit()

    write_reports(
        results_df=results_df,
        database_path=database_path,
        run_time_utc=run_time_utc,
    )

    required_failures = results_df[
        ~results_df["optional"]
        & (
            results_df["status"]
            != "created"
        )
    ]

    created_count = int(
        (
            results_df["status"]
            == "created"
        ).sum()
    )

    print()
    print("=" * 100)
    print("BACQE MACRO RESEARCH VIEWS BUILD COMPLETE")
    print("=" * 100)
    print(
        f"Views created:     "
        f"{created_count}/{len(results_df)}"
    )
    print(
        f"Required failures: "
        f"{len(required_failures)}"
    )
    print(f"Catalogue CSV:     {REPORT_CSV}")
    print(f"Report TXT:        {REPORT_TXT}")

    print()
    print(
        results_df[
            [
                "view_name",
                "source_table",
                "optional",
                "status",
                "row_count",
                "column_count",
                "min_date",
                "max_date",
            ]
        ].to_string(index=False)
    )

    if not required_failures.empty:
        print()
        print("Overall: FAILED")
        return 1

    print()
    print("Overall: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())