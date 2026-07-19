from __future__ import annotations

import json
import platform
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"
REPORT_DIR = PROJECT_ROOT / "reports"

REPORT_TXT = REPORT_DIR / "cftc_currency_mapping_resolution_latest.txt"
REPORT_CANDIDATES_CSV = (
    REPORT_DIR / "cftc_currency_mapping_candidates_latest.csv"
)
REPORT_RESOLUTION_CSV = (
    REPORT_DIR / "cftc_currency_mapping_resolution_latest.csv"
)
REPORT_AUDIT_CSV = (
    REPORT_DIR / "cftc_currency_mapping_audit_latest.csv"
)
REPORT_MANUAL_REVIEW_CSV = (
    REPORT_DIR / "cftc_currency_mapping_manual_review_latest.csv"
)


CURRENCY_REGISTRY_TABLE = "currency_registry"
CFTC_MAPPING_TABLE = "cftc_market_mapping"

SQLITE_CANDIDATES_TABLE = "cftc_currency_mapping_candidates"
SQLITE_RESOLUTION_TABLE = "cftc_currency_mapping_resolution"
SQLITE_AUDIT_TABLE = "cftc_currency_mapping_audit"
SQLITE_MANUAL_REVIEW_TABLE = "cftc_currency_mapping_manual_review"
SQLITE_SUMMARY_TABLE = "cftc_currency_mapping_resolution_summary"


TARGET_CURRENCIES = [
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "AUD",
    "CAD",
    "CHF",
    "NZD",
    "CNY",
]


CURRENCY_LABELS = {
    "USD": [
        "US DOLLAR",
        "U.S. DOLLAR",
        "UNITED STATES DOLLAR",
        "US DOLLAR INDEX",
        "U.S. DOLLAR INDEX",
        "USD INDEX",
        "DOLLAR INDEX",
        "USD",
    ],
    "EUR": [
        "EURO FX",
        "EURO",
        "EUROPEAN CURRENCY UNIT",
        "EUR",
    ],
    "GBP": [
        "BRITISH POUND",
        "POUND STERLING",
        "STERLING",
        "GBP",
    ],
    "JPY": [
        "JAPANESE YEN",
        "YEN",
        "JPY",
    ],
    "AUD": [
        "AUSTRALIAN DOLLAR",
        "AUSSIE DOLLAR",
        "AUD",
    ],
    "CAD": [
        "CANADIAN DOLLAR",
        "CAD",
    ],
    "CHF": [
        "SWISS FRANC",
        "FRANC SUISSE",
        "CHF",
    ],
    "NZD": [
        "NEW ZEALAND DOLLAR",
        "NEW ZEALAND DLR",
        "NZ DOLLAR",
        "KIWI DOLLAR",
        "NZD",
    ],
    "CNY": [
        "CHINESE RENMINBI",
        "CHINESE YUAN",
        "RENMINBI",
        "YUAN",
        "CNY",
        "CNH",
    ],
}


KNOWN_CFTC_IDENTITIES = {
    "EUR": [
        "EURO FX",
    ],
    "GBP": [
        "BRITISH POUND",
    ],
    "JPY": [
        "JAPANESE YEN",
    ],
    "AUD": [
        "AUSTRALIAN DOLLAR",
    ],
    "CAD": [
        "CANADIAN DOLLAR",
    ],
    "CHF": [
        "SWISS FRANC",
    ],
    "NZD": [
        "NEW ZEALAND DOLLAR",
        "NEW ZEALAND DLR",
    ],
    "USD": [
        "U.S. DOLLAR INDEX",
        "US DOLLAR INDEX",
        "USD INDEX",
    ],
}


MARKET_COLUMN_CANDIDATES = [
    "market_and_exchange_names",
    "market_and_exchange_name",
    "market_name",
    "contract_market_name",
    "commodity_name",
    "commodity",
    "market",
    "contract_name",
    "instrument_name",
    "name",
]


CONTRACT_CODE_COLUMN_CANDIDATES = [
    "cftc_contract_market_code",
    "contract_market_code",
    "cftc_market_code",
    "contract_code",
    "market_code",
]


REPORT_DATE_COLUMN_CANDIDATES = [
    "report_date_as_yyyy_mm_dd",
    "report_date",
    "observation_date",
    "as_of_date",
    "date",
]


EXCHANGE_COLUMN_CANDIDATES = [
    "exchange",
    "exchange_name",
    "contract_market",
]


EXCLUSION_TERMS = {
    "BITCOIN",
    "ETHER",
    "CRYPTO",
    "RUSSIAN RUBLE",
    "MEXICAN PESO",
    "BRAZILIAN REAL",
    "SOUTH AFRICAN RAND",
    "NORWEGIAN KRONE",
    "SWEDISH KRONA",
    "POLISH ZLOTY",
    "INDIAN RUPEE",
    "KOREAN WON",
    "CANADIAN HOUSING",
    "EURODOLLAR",
}


@dataclass(frozen=True)
class SourceObject:
    object_name: str
    market_column: str
    report_date_column: str | None
    contract_code_column: str | None
    exchange_column: str | None
    row_count: int
    market_count: int
    suitability_score: int


def load_config() -> dict[str, Any]:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {CONFIG_FILE}"
        )

    with CONFIG_FILE.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(
            f"Invalid configuration structure: {CONFIG_FILE}"
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

    if "data_lake_root" not in config:
        raise KeyError(
            "'data_lake_root' is missing from config/paths.yaml"
        )

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

    path = Path(linux_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Configured Linux Data Lake does not exist: {path}"
        )

    return path


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


def get_output_dir(
    data_lake_root: Path,
) -> Path:
    output_dir = (
        data_lake_root
        / "data"
        / "curated"
        / "macro"
        / "cftc_mapping_resolution"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    return output_dir


def database_object_exists(
    connection: sqlite3.Connection,
    object_name: str,
) -> bool:
    result = connection.execute(
        """
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE name = ?
        """,
        [object_name],
    ).fetchone()

    return bool(result and result[0] > 0)


def read_sql_object(
    connection: sqlite3.Connection,
    object_name: str,
    required: bool = True,
) -> pd.DataFrame:
    if not database_object_exists(
        connection,
        object_name,
    ):
        if required:
            raise ValueError(
                f"Required database object is missing: {object_name}"
            )

        return pd.DataFrame()

    return pd.read_sql_query(
        f'SELECT * FROM "{object_name}"',
        connection,
    )


def get_database_objects(
    connection: sqlite3.Connection,
) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            name AS object_name,
            type AS object_type
        FROM sqlite_master
        WHERE type IN ('table', 'view')
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """,
        connection,
    )


def get_object_columns(
    connection: sqlite3.Connection,
    object_name: str,
) -> list[str]:
    rows = connection.execute(
        f'PRAGMA table_info("{object_name}")'
    ).fetchall()

    return [
        str(row[1])
        for row in rows
    ]


def first_existing_column(
    columns: list[str],
    candidates: list[str],
) -> str | None:
    normalised = {
        column.lower(): column
        for column in columns
    }

    for candidate in candidates:
        if candidate.lower() in normalised:
            return normalised[candidate.lower()]

    return None


def safe_count_rows(
    connection: sqlite3.Connection,
    object_name: str,
) -> int:
    try:
        result = connection.execute(
            f'SELECT COUNT(*) FROM "{object_name}"'
        ).fetchone()

        return int(result[0]) if result else 0

    except sqlite3.DatabaseError:
        return 0


def safe_count_distinct(
    connection: sqlite3.Connection,
    object_name: str,
    column_name: str,
) -> int:
    try:
        result = connection.execute(
            f"""
            SELECT COUNT(DISTINCT "{column_name}")
            FROM "{object_name}"
            WHERE "{column_name}" IS NOT NULL
            """
        ).fetchone()

        return int(result[0]) if result else 0

    except sqlite3.DatabaseError:
        return 0


def source_object_suitability_score(
    object_name: str,
    columns: list[str],
    market_column: str,
    report_date_column: str | None,
    contract_code_column: str | None,
    row_count: int,
    market_count: int,
) -> int:
    name = object_name.lower()

    score = 0

    if "cftc" in name:
        score += 100

    if "cot" in name:
        score += 60

    if "tff" in name:
        score += 50

    if "latest" in name:
        score += 15

    if "raw" in name:
        score += 5

    if market_column:
        score += 50

    if report_date_column:
        score += 20

    if contract_code_column:
        score += 20

    lower_columns = {
        column.lower()
        for column in columns
    }

    leveraged_terms = [
        "leveraged",
        "lev_money",
    ]

    asset_manager_terms = [
        "asset_mgr",
        "asset_manager",
    ]

    if any(
        term in column
        for column in lower_columns
        for term in leveraged_terms
    ):
        score += 25

    if any(
        term in column
        for column in lower_columns
        for term in asset_manager_terms
    ):
        score += 25

    if row_count > 0:
        score += min(
            int(np.log10(row_count + 1) * 5),
            30,
        )

    if market_count >= 5:
        score += 20

    return score


def discover_cftc_source_objects(
    connection: sqlite3.Connection,
) -> pd.DataFrame:
    objects = get_database_objects(
        connection
    )

    discovered: list[dict[str, Any]] = []

    for _, object_row in objects.iterrows():
        object_name = str(
            object_row["object_name"]
        )

        columns = get_object_columns(
            connection,
            object_name,
        )

        market_column = first_existing_column(
            columns,
            MARKET_COLUMN_CANDIDATES,
        )

        if market_column is None:
            continue

        object_name_lower = object_name.lower()
        columns_lower = " ".join(
            column.lower()
            for column in columns
        )

        likely_cftc = (
            "cftc" in object_name_lower
            or "cot" in object_name_lower
            or "leveraged" in columns_lower
            or "asset_mgr" in columns_lower
            or "asset_manager" in columns_lower
        )

        if not likely_cftc:
            continue

        report_date_column = first_existing_column(
            columns,
            REPORT_DATE_COLUMN_CANDIDATES,
        )

        contract_code_column = first_existing_column(
            columns,
            CONTRACT_CODE_COLUMN_CANDIDATES,
        )

        exchange_column = first_existing_column(
            columns,
            EXCHANGE_COLUMN_CANDIDATES,
        )

        row_count = safe_count_rows(
            connection,
            object_name,
        )

        market_count = safe_count_distinct(
            connection,
            object_name,
            market_column,
        )

        score = source_object_suitability_score(
            object_name=object_name,
            columns=columns,
            market_column=market_column,
            report_date_column=report_date_column,
            contract_code_column=contract_code_column,
            row_count=row_count,
            market_count=market_count,
        )

        discovered.append(
            {
                "object_name": object_name,
                "object_type": object_row["object_type"],
                "market_column": market_column,
                "report_date_column": report_date_column,
                "contract_code_column": contract_code_column,
                "exchange_column": exchange_column,
                "row_count": row_count,
                "market_count": market_count,
                "suitability_score": score,
                "columns_json": json.dumps(
                    columns
                ),
            }
        )

    discovered_df = pd.DataFrame(
        discovered
    )

    if discovered_df.empty:
        raise ValueError(
            "No suitable CFTC source object was discovered "
            "inside the Macro Master Database."
        )

    return discovered_df.sort_values(
        [
            "suitability_score",
            "row_count",
            "object_name",
        ],
        ascending=[
            False,
            False,
            True,
        ],
    ).reset_index(drop=True)


def select_source_object(
    discovered: pd.DataFrame,
) -> SourceObject:
    row = discovered.iloc[0]

    return SourceObject(
        object_name=str(row["object_name"]),
        market_column=str(row["market_column"]),
        report_date_column=(
            str(row["report_date_column"])
            if pd.notna(row["report_date_column"])
            else None
        ),
        contract_code_column=(
            str(row["contract_code_column"])
            if pd.notna(row["contract_code_column"])
            else None
        ),
        exchange_column=(
            str(row["exchange_column"])
            if pd.notna(row["exchange_column"])
            else None
        ),
        row_count=int(row["row_count"]),
        market_count=int(row["market_count"]),
        suitability_score=int(
            row["suitability_score"]
        ),
    )


def load_market_universe(
    connection: sqlite3.Connection,
    source: SourceObject,
) -> pd.DataFrame:
    select_columns = [
        f'"{source.market_column}" AS market_name'
    ]

    if source.contract_code_column:
        select_columns.append(
            f'"{source.contract_code_column}" '
            "AS contract_market_code"
        )
    else:
        select_columns.append(
            "NULL AS contract_market_code"
        )

    if source.exchange_column:
        select_columns.append(
            f'"{source.exchange_column}" AS exchange_name'
        )
    else:
        select_columns.append(
            "NULL AS exchange_name"
        )

    if source.report_date_column:
        select_columns.append(
            f'MAX("{source.report_date_column}") '
            "AS latest_report_date"
        )
    else:
        select_columns.append(
            "NULL AS latest_report_date"
        )

    group_columns = [
        f'"{source.market_column}"'
    ]

    if source.contract_code_column:
        group_columns.append(
            f'"{source.contract_code_column}"'
        )

    if source.exchange_column:
        group_columns.append(
            f'"{source.exchange_column}"'
        )

    query = f"""
        SELECT
            {", ".join(select_columns)},
            COUNT(*) AS source_row_count
        FROM "{source.object_name}"
        WHERE "{source.market_column}" IS NOT NULL
        GROUP BY {", ".join(group_columns)}
        ORDER BY "{source.market_column}"
    """

    market_universe = pd.read_sql_query(
        query,
        connection,
    )

    market_universe["market_name"] = (
        market_universe["market_name"]
        .astype("string")
        .str.strip()
    )

    market_universe = market_universe[
        market_universe["market_name"].notna()
    ].copy()

    market_universe = market_universe[
        market_universe["market_name"].str.len() > 0
    ].copy()

    market_universe["latest_report_date"] = (
        pd.to_datetime(
            market_universe["latest_report_date"],
            errors="coerce",
            utc=True,
        )
    )

    return market_universe.reset_index(
        drop=True
    )


def normalise_text(
    value: Any,
) -> str:
    if value is None or pd.isna(value):
        return ""

    text = str(value)

    text = unicodedata.normalize(
        "NFKD",
        text,
    )

    text = "".join(
        character
        for character in text
        if not unicodedata.combining(
            character
        )
    )

    text = text.upper()

    replacements = {
        "&": " AND ",
        "U.S.": " US ",
        "U S": " US ",
        "D.L.R.": " DOLLAR ",
        "DLR": " DOLLAR ",
        "STERLING": " BRITISH POUND ",
        "AUSSIE": " AUSTRALIAN ",
        "KIWI": " NEW ZEALAND ",
        "RENMINBI": " YUAN ",
    }

    for old, new in replacements.items():
        text = text.replace(
            old,
            new,
        )

    text = re.sub(
        r"[^A-Z0-9]+",
        " ",
        text,
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip()

    return text


def extract_contract_identity(
    market_name: Any,
) -> str:
    """
    Remove the exchange suffix from a full CFTC market name.

    Example:
        AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE

    becomes:
        AUSTRALIAN DOLLAR

    The original market name remains available elsewhere for reporting.
    Only the cleaned contract identity is used for similarity scoring.
    """
    normalised = normalise_text(
        market_name
    )

    exchange_suffixes = [
        " CHICAGO MERCANTILE EXCHANGE",
        " CHICAGO BOARD OF TRADE",
        " ICE FUTURES US",
        " CBOE FUTURES EXCHANGE",
        " COINBASE DERIVATIVES LLC",
        " LMX LABS LLC",
        " NEW YORK MERCANTILE EXCHANGE",
        " COMMODITY EXCHANGE INC",
        " ICE FUTURES EUROPE",
        " NASDAQ FUTURES INC",
    ]

    for suffix in exchange_suffixes:
        if normalised.endswith(
            suffix
        ):
            normalised = normalised[
                : -len(suffix)
            ].strip()

            break

    return normalised


def tokenise(
    value: Any,
) -> set[str]:
    normalised = normalise_text(
        value
    )

    stopwords = {
        "AND",
        "THE",
        "OF",
        "FUTURES",
        "CONTRACT",
        "CONTRACTS",
        "EXCHANGE",
        "CME",
        "IMM",
        "ICE",
        "COMMODITY",
    }

    return {
        token
        for token in normalised.split()
        if token not in stopwords
    }


def sequence_similarity(
    left: str,
    right: str,
) -> float:
    return SequenceMatcher(
        None,
        left,
        right,
    ).ratio()


def token_overlap_score(
    left: str,
    right: str,
) -> float:
    left_tokens = tokenise(
        left
    )

    right_tokens = tokenise(
        right
    )

    if not left_tokens or not right_tokens:
        return 0.0

    intersection = len(
        left_tokens.intersection(
            right_tokens
        )
    )

    union = len(
        left_tokens.union(
            right_tokens
        )
    )

    return (
        intersection / union
        if union
        else 0.0
    )


def contains_alias(
    market_name: str,
    alias: str,
) -> bool:
    market_normalised = normalise_text(
        market_name
    )

    alias_normalised = normalise_text(
        alias
    )

    if not alias_normalised:
        return False

    return (
        alias_normalised == market_normalised
        or alias_normalised in market_normalised
    )


def is_excluded_market(
    market_name: str,
    currency_code: str,
) -> bool:
    normalised = normalise_text(
        market_name
    )

    if currency_code == "CNY":
        return False

    return any(
        exclusion in normalised
        for exclusion in EXCLUSION_TERMS
    )


def get_registry_mapping_columns(
    mapping_registry: pd.DataFrame,
) -> dict[str, str | None]:
    columns = list(
        mapping_registry.columns
    )

    return {
        "currency": first_existing_column(
            columns,
            [
                "currency_code",
                "currency",
                "mapped_currency",
            ],
        ),
        "match_term": first_existing_column(
            columns,
            [
                "market_match_term",
                "match_term",
                "cftc_market_name",
                "market_name",
                "contract_name",
            ],
        ),
        "contract_code": first_existing_column(
            columns,
            [
                "cftc_contract_market_code",
                "contract_market_code",
                "contract_code",
                "market_code",
            ],
        ),
        "active": first_existing_column(
            columns,
            [
                "is_active",
                "active",
                "enabled",
            ],
        ),
    }


def get_existing_mapping(
    mapping_registry: pd.DataFrame,
    currency_code: str,
) -> dict[str, Any]:
    if mapping_registry.empty:
        return {
            "mapping_exists": False,
            "existing_match_term": None,
            "existing_contract_code": None,
            "existing_mapping_active": None,
        }

    columns = get_registry_mapping_columns(
        mapping_registry
    )

    currency_column = columns["currency"]

    if currency_column is None:
        return {
            "mapping_exists": False,
            "existing_match_term": None,
            "existing_contract_code": None,
            "existing_mapping_active": None,
        }

    matches = mapping_registry[
        mapping_registry[currency_column]
        .astype("string")
        .str.upper()
        .str.strip()
        .eq(currency_code)
    ]

    if matches.empty:
        return {
            "mapping_exists": False,
            "existing_match_term": None,
            "existing_contract_code": None,
            "existing_mapping_active": None,
        }

    row = matches.iloc[0]

    return {
        "mapping_exists": True,
        "existing_match_term": (
            row.get(columns["match_term"])
            if columns["match_term"]
            else None
        ),
        "existing_contract_code": (
            row.get(columns["contract_code"])
            if columns["contract_code"]
            else None
        ),
        "existing_mapping_active": (
            row.get(columns["active"])
            if columns["active"]
            else None
        ),
    }


def candidate_aliases(
    currency_code: str,
    existing_match_term: Any,
) -> list[str]:
    aliases: list[str] = []

    if (
        existing_match_term is not None
        and not pd.isna(existing_match_term)
        and str(existing_match_term).strip()
    ):
        aliases.append(
            str(existing_match_term)
        )

    aliases.extend(
        CURRENCY_LABELS.get(
            currency_code,
            [],
        )
    )

    aliases.extend(
        KNOWN_CFTC_IDENTITIES.get(
            currency_code,
            [],
        )
    )

    unique_aliases: list[str] = []
    seen: set[str] = set()

    for alias in aliases:
        key = normalise_text(
            alias
        )

        if key and key not in seen:
            seen.add(
                key
            )
            unique_aliases.append(
                alias
            )

    return unique_aliases


def score_candidate(
    currency_code: str,
    market_name: str,
    aliases: list[str],
    existing_match_term: Any,
    existing_contract_code: Any,
    market_contract_code: Any,
) -> dict[str, Any]:
    market_normalised_full = normalise_text(
        market_name
    )

    market_normalised = extract_contract_identity(
        market_name
    )

    best_alias = ""
    best_sequence = 0.0
    best_token = 0.0
    alias_contained = False
    exact_alias_match = False

    for alias in aliases:
        alias_normalised = normalise_text(
            alias
        )

        sequence_score = sequence_similarity(
            alias_normalised,
            market_normalised,
        )

        token_score = token_overlap_score(
            alias_normalised,
            market_normalised,
        )

        if (
            sequence_score + token_score
            > best_sequence + best_token
        ):
            best_alias = alias
            best_sequence = sequence_score
            best_token = token_score

        if alias_normalised == market_normalised:
            exact_alias_match = True
            best_alias = alias
            best_sequence = 1.0
            best_token = 1.0

        if contains_alias(
            market_name,
            alias,
        ):
            alias_contained = True

    existing_term_exact = False
    existing_term_contained = False

    if (
        existing_match_term is not None
        and not pd.isna(existing_match_term)
    ):
        existing_normalised = normalise_text(
            existing_match_term
        )

        existing_term_exact = (
            existing_normalised
            == market_normalised
        )

        existing_term_contained = (
            existing_normalised
            and existing_normalised
            in market_normalised
        )

    contract_code_match = False

    if (
        existing_contract_code is not None
        and not pd.isna(existing_contract_code)
        and market_contract_code is not None
        and not pd.isna(market_contract_code)
    ):
        contract_code_match = (
            normalise_text(existing_contract_code)
            == normalise_text(market_contract_code)
        )

    score = (
        best_sequence * 45
        + best_token * 30
    )

    if alias_contained:
        score += 15

    if exact_alias_match:
        score += 25

    if existing_term_contained:
        score += 10

    if existing_term_exact:
        score += 20

    if contract_code_match:
        score += 40

    if is_excluded_market(
        market_name,
        currency_code,
    ):
        score -= 100

    if currency_code == "USD":
        if (
                "DOLLAR INDEX" in market_normalised
                or "USD INDEX" in market_normalised
        ):
            score += 20

        if "EURODOLLAR" in market_normalised_full:
            score -= 100

    if currency_code == "CNY":
        if any(
                token in market_normalised_full
                for token in [
                    "CHINESE",
                    "YUAN",
                    "CNY",
                    "CNH",
                    "RENMINBI",
                ]
        ):
            score += 15

    score = max(
        min(score, 100.0),
        0.0,
    )

    return {
        "best_alias": best_alias,
        "sequence_similarity": round(
            best_sequence,
            4,
        ),
        "token_overlap": round(
            best_token,
            4,
        ),
        "alias_contained": alias_contained,
        "exact_alias_match": exact_alias_match,
        "existing_term_exact": existing_term_exact,
        "existing_term_contained": existing_term_contained,
        "contract_code_match": contract_code_match,
        "candidate_score": round(
            score,
            2,
        ),
    }


def build_candidates(
    market_universe: pd.DataFrame,
    mapping_registry: pd.DataFrame,
    source: SourceObject,
    generated_utc: datetime,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for currency_code in TARGET_CURRENCIES:
        existing = get_existing_mapping(
            mapping_registry,
            currency_code,
        )

        aliases = candidate_aliases(
            currency_code,
            existing["existing_match_term"],
        )

        currency_candidates: list[dict[str, Any]] = []

        for _, market_row in market_universe.iterrows():
            scored = score_candidate(
                currency_code=currency_code,
                market_name=str(
                    market_row["market_name"]
                ),
                aliases=aliases,
                existing_match_term=existing[
                    "existing_match_term"
                ],
                existing_contract_code=existing[
                    "existing_contract_code"
                ],
                market_contract_code=market_row.get(
                    "contract_market_code"
                ),
            )

            if scored["candidate_score"] <= 0:
                continue

            candidate = {
                "currency_code": currency_code,
                "mapping_exists": existing[
                    "mapping_exists"
                ],
                "existing_match_term": existing[
                    "existing_match_term"
                ],
                "existing_contract_code": existing[
                    "existing_contract_code"
                ],
                "existing_mapping_active": existing[
                    "existing_mapping_active"
                ],
                "candidate_market_name": market_row[
                    "market_name"
                ],
                "candidate_contract_identity": extract_contract_identity(
                    market_row["market_name"]
                ),
                "candidate_contract_code": market_row.get(
                    "contract_market_code"
                ),
                "candidate_exchange": market_row.get(
                    "exchange_name"
                ),
                "candidate_latest_report_date": market_row.get(
                    "latest_report_date"
                ),
                "candidate_source_row_count": market_row.get(
                    "source_row_count"
                ),
                "source_object": source.object_name,
                "source_market_column": source.market_column,
                "aliases_tested": json.dumps(
                    aliases,
                    ensure_ascii=False,
                ),
                **scored,
                "generated_utc": generated_utc.isoformat(),
            }

            currency_candidates.append(
                candidate
            )

        currency_candidates.sort(
            key=lambda item: (
                item["candidate_score"],
                item["candidate_source_row_count"]
                if pd.notna(
                    item["candidate_source_row_count"]
                )
                else 0,
            ),
            reverse=True,
        )

        for rank, candidate in enumerate(
            currency_candidates[:10],
            start=1,
        ):
            candidate["candidate_rank"] = rank
            rows.append(
                candidate
            )

    candidates = pd.DataFrame(
        rows
    )

    if candidates.empty:
        return pd.DataFrame(
            columns=[
                "currency_code",
                "candidate_rank",
                "candidate_market_name",
                "candidate_score",
            ]
        )

    return candidates.sort_values(
        [
            "currency_code",
            "candidate_rank",
        ]
    ).reset_index(drop=True)


def determine_resolution_status(
    currency_code: str,
    top_candidate: pd.Series | None,
    second_candidate: pd.Series | None,
    mapping_exists: bool,
) -> tuple[str, str, bool]:
    if top_candidate is None:
        if currency_code == "CNY":
            return (
                "no_comparable_contract",
                (
                    "No directly comparable standalone CFTC currency-positioning "
                    "contract was identified for CNY."
                ),
                False,
            )

        return (
            "manual_review_required",
            "No candidate market reached a usable similarity score.",
            False,
        )

    top_score = float(
        top_candidate["candidate_score"]
    )

    second_score = (
        float(second_candidate["candidate_score"])
        if second_candidate is not None
        else 0.0
    )

    score_margin = (
        top_score - second_score
    )

    exact_evidence = bool(
        top_candidate.get(
            "exact_alias_match",
            False,
        )
        or top_candidate.get(
            "existing_term_exact",
            False,
        )
        or top_candidate.get(
            "contract_code_match",
            False,
        )
    )

    alias_evidence = bool(
        top_candidate.get(
            "alias_contained",
            False,
        )
        or top_candidate.get(
            "existing_term_contained",
            False,
        )
    )

    if (
        top_score >= 95
        and exact_evidence
        and score_margin >= 5
    ):
        return (
            "resolved_exact",
            "The top market has exact name or contract-code evidence and a clear lead over alternatives.",
            True,
        )

    if (
        top_score >= 85
        and alias_evidence
        and score_margin >= 10
    ):
        return (
            "resolved_alias",
            "The top market has strong alias evidence and a clear lead over alternatives.",
            True,
        )

    if currency_code == "CNY" and top_score < 75:
        return (
            "no_comparable_contract",
            (
                "A related offshore renminbi cross may exist, but it is not "
                "treated as a directly comparable standalone CFTC "
                "currency-positioning contract."
            ),
            False,
        )

    if mapping_exists and top_score >= 75:
        return (
            "manual_review_required",
            "A plausible candidate exists, but the current registry mapping should be reviewed before amendment.",
            False,
        )

    return (
        "manual_review_required",
        "Candidate evidence is insufficient for automatic resolution.",
        False,
    )


def build_resolution(
    candidates: pd.DataFrame,
    mapping_registry: pd.DataFrame,
    generated_utc: datetime,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for currency_code in TARGET_CURRENCIES:
        existing = get_existing_mapping(
            mapping_registry,
            currency_code,
        )

        currency_candidates = candidates[
            candidates["currency_code"]
            == currency_code
        ].sort_values(
            "candidate_rank"
        )

        top_candidate = (
            currency_candidates.iloc[0]
            if len(currency_candidates) >= 1
            else None
        )

        second_candidate = (
            currency_candidates.iloc[1]
            if len(currency_candidates) >= 2
            else None
        )

        (
            resolution_status,
            resolution_reason,
            safe_to_apply,
        ) = determine_resolution_status(
            currency_code=currency_code,
            top_candidate=top_candidate,
            second_candidate=second_candidate,
            mapping_exists=existing[
                "mapping_exists"
            ],
        )

        top_score = (
            float(
                top_candidate[
                    "candidate_score"
                ]
            )
            if top_candidate is not None
            else np.nan
        )

        second_score = (
            float(
                second_candidate[
                    "candidate_score"
                ]
            )
            if second_candidate is not None
            else np.nan
        )

        score_margin = (
            top_score - second_score
            if pd.notna(top_score)
            and pd.notna(second_score)
            else np.nan
        )

        recommended_market_name = (
            top_candidate[
                "candidate_market_name"
            ]
            if top_candidate is not None
            else None
        )

        recommended_contract_code = (
            top_candidate[
                "candidate_contract_code"
            ]
            if top_candidate is not None
            else None
        )

        current_mapping_matches = False

        if (
            top_candidate is not None
            and existing["existing_match_term"]
            is not None
            and not pd.isna(
                existing["existing_match_term"]
            )
        ):
            current_mapping_matches = (
                normalise_text(
                    existing[
                        "existing_match_term"
                    ]
                )
                in normalise_text(
                    recommended_market_name
                )
            )

        mapping_change_required = bool(
            safe_to_apply
            and not current_mapping_matches
        )

        rows.append(
            {
                "currency_code": currency_code,
                "mapping_exists": existing[
                    "mapping_exists"
                ],
                "existing_match_term": existing[
                    "existing_match_term"
                ],
                "existing_contract_code": existing[
                    "existing_contract_code"
                ],
                "recommended_market_name": recommended_market_name,
                "recommended_contract_code": recommended_contract_code,
                "top_candidate_score": top_score,
                "second_candidate_score": second_score,
                "score_margin": score_margin,
                "resolution_status": resolution_status,
                "resolution_reason": resolution_reason,
                "safe_to_apply": safe_to_apply,
                "current_mapping_matches": current_mapping_matches,
                "mapping_change_required": mapping_change_required,
                "generated_utc": generated_utc.isoformat(),
            }
        )

    return pd.DataFrame(
        rows
    )


def build_audit(
    resolution: pd.DataFrame,
    generated_utc: datetime,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for _, row in resolution.iterrows():
        status = str(
            row["resolution_status"]
        )

        if status == "resolved_exact":
            severity = "informational"
            audit_result = "pass"

        elif status == "resolved_alias":
            severity = "low"
            audit_result = "pass_with_recommendation"

        elif status == "no_comparable_contract":
            severity = (
                "informational"
                if row["currency_code"] == "CNY"
                else "medium"
            )
            audit_result = "documented_exception"

        else:
            severity = "high"
            audit_result = "manual_review_required"

        rows.append(
            {
                "currency_code": row[
                    "currency_code"
                ],
                "resolution_status": status,
                "audit_result": audit_result,
                "severity": severity,
                "mapping_exists": row[
                    "mapping_exists"
                ],
                "existing_match_term": row[
                    "existing_match_term"
                ],
                "recommended_market_name": row[
                    "recommended_market_name"
                ],
                "top_candidate_score": row[
                    "top_candidate_score"
                ],
                "score_margin": row[
                    "score_margin"
                ],
                "safe_to_apply": row[
                    "safe_to_apply"
                ],
                "mapping_change_required": row[
                    "mapping_change_required"
                ],
                "audit_note": row[
                    "resolution_reason"
                ],
                "generated_utc": generated_utc.isoformat(),
            }
        )

    return pd.DataFrame(
        rows
    )


def build_manual_review(
    candidates: pd.DataFrame,
    resolution: pd.DataFrame,
) -> pd.DataFrame:
    review_currencies = resolution.loc[
        resolution["resolution_status"]
        == "manual_review_required",
        "currency_code",
    ].tolist()

    if not review_currencies:
        return pd.DataFrame(
            columns=[
                "currency_code",
                "candidate_rank",
                "candidate_market_name",
                "candidate_score",
            ]
        )

    manual_review = candidates[
        candidates["currency_code"]
        .isin(review_currencies)
    ].copy()

    return manual_review[
        manual_review["candidate_rank"] <= 5
    ].reset_index(drop=True)


def build_summary(
    source: SourceObject,
    market_universe: pd.DataFrame,
    candidates: pd.DataFrame,
    resolution: pd.DataFrame,
    generated_utc: datetime,
) -> pd.DataFrame:
    status_counts = (
        resolution["resolution_status"]
        .value_counts()
        .to_dict()
    )

    return pd.DataFrame(
        [
            {
                "generated_utc": generated_utc.isoformat(),
                "source_object": source.object_name,
                "source_market_column": source.market_column,
                "source_rows": source.row_count,
                "distinct_market_rows": len(
                    market_universe
                ),
                "candidate_rows": len(
                    candidates
                ),
                "currencies_assessed": len(
                    resolution
                ),
                "resolved_exact": int(
                    status_counts.get(
                        "resolved_exact",
                        0,
                    )
                ),
                "resolved_alias": int(
                    status_counts.get(
                        "resolved_alias",
                        0,
                    )
                ),
                "manual_review_required": int(
                    status_counts.get(
                        "manual_review_required",
                        0,
                    )
                ),
                "no_comparable_contract": int(
                    status_counts.get(
                        "no_comparable_contract",
                        0,
                    )
                ),
                "safe_mapping_changes": int(
                    resolution[
                        "mapping_change_required"
                    ].sum()
                ),
            }
        ]
    )


def validate_outputs(
    candidates: pd.DataFrame,
    resolution: pd.DataFrame,
    audit: pd.DataFrame,
) -> None:
    if resolution.empty:
        raise ValueError(
            "Resolution output contains no rows."
        )

    if len(resolution) != len(
        TARGET_CURRENCIES
    ):
        raise ValueError(
            "Unexpected resolution row count. "
            f"Expected {len(TARGET_CURRENCIES)}, "
            f"received {len(resolution)}."
        )

    duplicates = resolution.duplicated(
        subset=["currency_code"],
        keep=False,
    )

    if duplicates.any():
        duplicate_codes = resolution.loc[
            duplicates,
            "currency_code",
        ].tolist()

        raise ValueError(
            "Duplicate currency resolutions found: "
            f"{duplicate_codes}"
        )

    valid_statuses = {
        "resolved_exact",
        "resolved_alias",
        "manual_review_required",
        "no_comparable_contract",
    }

    invalid_statuses = set(
        resolution[
            "resolution_status"
        ].dropna()
    ).difference(
        valid_statuses
    )

    if invalid_statuses:
        raise ValueError(
            "Invalid resolution statuses found: "
            f"{sorted(invalid_statuses)}"
        )

    if len(audit) != len(resolution):
        raise ValueError(
            "Audit row count does not match resolution row count."
        )

    if not candidates.empty:
        duplicate_ranks = candidates.duplicated(
            subset=[
                "currency_code",
                "candidate_rank",
            ],
            keep=False,
        )

        if duplicate_ranks.any():
            raise ValueError(
                "Duplicate candidate ranks detected."
            )


def make_sqlite_safe(
    df: pd.DataFrame,
) -> pd.DataFrame:
    output = df.copy()

    for column in output.columns:
        series = output[column]

        if pd.api.types.is_datetime64_any_dtype(
            series
        ):
            converted = pd.to_datetime(
                series,
                errors="coerce",
                utc=True,
            )

            output[column] = (
                converted
                .dt.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                .where(
                    converted.notna(),
                    None,
                )
            )

            continue

        if pd.api.types.is_bool_dtype(
            series
        ):
            output[column] = (
                series
                .astype("Int64")
            )

            continue

        if series.dtype == "object":
            output[column] = series.map(
                lambda value: (
                    value.isoformat()
                    if isinstance(
                        value,
                        (
                            pd.Timestamp,
                            datetime,
                        ),
                    )
                    else value
                )
            )

    return output


def save_sqlite_tables(
    connection: sqlite3.Connection,
    candidates: pd.DataFrame,
    resolution: pd.DataFrame,
    audit: pd.DataFrame,
    manual_review: pd.DataFrame,
    summary: pd.DataFrame,
) -> None:
    tables = {
        SQLITE_CANDIDATES_TABLE: candidates,
        SQLITE_RESOLUTION_TABLE: resolution,
        SQLITE_AUDIT_TABLE: audit,
        SQLITE_MANUAL_REVIEW_TABLE: manual_review,
        SQLITE_SUMMARY_TABLE: summary,
    }

    for table_name, dataframe in tables.items():
        make_sqlite_safe(
            dataframe
        ).to_sql(
            table_name,
            connection,
            if_exists="replace",
            index=False,
        )


def save_dataframe_outputs(
    df: pd.DataFrame,
    output_dir: Path,
    filename_stem: str,
    timestamp: str,
) -> tuple[Path, Path, Path, Path]:
    dated_csv = (
        output_dir
        / f"{filename_stem}_{timestamp}.csv"
    )

    dated_parquet = (
        output_dir
        / f"{filename_stem}_{timestamp}.parquet"
    )

    latest_csv = (
        output_dir
        / f"{filename_stem}_latest.csv"
    )

    latest_parquet = (
        output_dir
        / f"{filename_stem}_latest.parquet"
    )

    df.to_csv(
        dated_csv,
        index=False,
    )

    df.to_parquet(
        dated_parquet,
        index=False,
    )

    df.to_csv(
        latest_csv,
        index=False,
    )

    df.to_parquet(
        latest_parquet,
        index=False,
    )

    return (
        dated_csv,
        dated_parquet,
        latest_csv,
        latest_parquet,
    )


def write_reports(
    source: SourceObject,
    discovered_sources: pd.DataFrame,
    market_universe: pd.DataFrame,
    resolution: pd.DataFrame,
    manual_review: pd.DataFrame,
    summary: pd.DataFrame,
    database_path: Path,
    output_dir: Path,
    generated_utc: datetime,
) -> None:
    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    resolution.to_csv(
        REPORT_RESOLUTION_CSV,
        index=False,
    )

    manual_review.to_csv(
        REPORT_MANUAL_REVIEW_CSV,
        index=False,
    )

    summary_row = summary.iloc[0]

    resolution_display_columns = [
        "currency_code",
        "mapping_exists",
        "existing_match_term",
        "recommended_market_name",
        "top_candidate_score",
        "score_margin",
        "resolution_status",
        "safe_to_apply",
        "mapping_change_required",
    ]

    source_display_columns = [
        "object_name",
        "market_column",
        "row_count",
        "market_count",
        "suitability_score",
    ]

    manual_display_columns = [
        "currency_code",
        "candidate_rank",
        "candidate_market_name",
        "candidate_contract_identity",
        "candidate_contract_code",
        "candidate_score",
        "best_alias",
    ]

    lines = [
        "=" * 122,
        "BACQE CFTC CURRENCY MAPPING RESOLUTION",
        "=" * 122,
        f"Generated UTC:        {generated_utc.isoformat()}",
        f"Database:             {database_path}",
        f"Output directory:     {output_dir}",
        "",
        f"Selected source:      {source.object_name}",
        f"Market column:        {source.market_column}",
        f"Source rows:          {source.row_count}",
        f"Distinct markets:     {len(market_universe)}",
        "",
        f"Currencies assessed:  {int(summary_row['currencies_assessed'])}",
        f"Resolved exact:       {int(summary_row['resolved_exact'])}",
        f"Resolved alias:       {int(summary_row['resolved_alias'])}",
        f"Manual review:        {int(summary_row['manual_review_required'])}",
        f"No comparable:        {int(summary_row['no_comparable_contract'])}",
        f"Safe changes:         {int(summary_row['safe_mapping_changes'])}",
        "",
        "-" * 122,
        "DISCOVERED CFTC SOURCE OBJECTS",
        "-" * 122,
        discovered_sources[
            source_display_columns
        ].head(10).to_string(
            index=False
        ),
        "",
        "-" * 122,
        "CURRENCY RESOLUTION",
        "-" * 122,
        resolution[
            resolution_display_columns
        ].to_string(
            index=False
        ),
        "",
        "-" * 122,
        "MANUAL REVIEW QUEUE",
        "-" * 122,
        (
            manual_review[
                manual_display_columns
            ].to_string(index=False)
            if not manual_review.empty
            else "No currencies require manual review."
        ),
        "",
        "=" * 122,
        "CALIBRATION RULES",
        "=" * 122,
        (
            "Exact or strong alias evidence may be recommended automatically only when the leading candidate "
            "has a clear margin over competing markets."
        ),
        (
            "Weak fuzzy similarity is never treated as sufficient evidence for an automatic mapping."
        ),
        (
            "CNY may remain explicitly unsupported when no comparable CFTC contract exists. "
            "Coverage is not improved by inventing an unsuitable proxy."
        ),
        (
            "This engine writes recommendations and audit evidence. It does not silently overwrite "
            "the canonical mapping registry."
        ),
        "",
        "=" * 122,
        "OVERALL RESULT",
        "=" * 122,
        "PASS — CFTC currency mapping calibration and resolution audit completed.",
    ]

    REPORT_TXT.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    print("=" * 112)
    print(
        "BACQE INFORMATION DATA 16 - "
        "RESOLVE CFTC CURRENCY MAPPINGS"
    )
    print("=" * 112)

    generated_utc = datetime.now(
        timezone.utc
    )

    timestamp = generated_utc.strftime(
        "%Y_%m_%d_%H%M%S"
    )

    data_lake_root = get_data_lake_root()

    database_path = get_database_path(
        data_lake_root
    )

    output_dir = get_output_dir(
        data_lake_root
    )

    if not database_path.exists():
        raise FileNotFoundError(
            "Macro Master Database not found: "
            f"{database_path}"
        )

    print(f"Data lake root: {data_lake_root}")
    print(f"Database:       {database_path}")
    print(f"Output dir:     {output_dir}")
    print("-" * 112)

    with sqlite3.connect(
        database_path
    ) as connection:
        mapping_registry = read_sql_object(
            connection,
            CFTC_MAPPING_TABLE,
            required=False,
        )

        read_sql_object(
            connection,
            CURRENCY_REGISTRY_TABLE,
            required=False,
        )

        discovered_sources = (
            discover_cftc_source_objects(
                connection
            )
        )

        source = select_source_object(
            discovered_sources
        )

        market_universe = load_market_universe(
            connection,
            source,
        )

        candidates = build_candidates(
            market_universe=market_universe,
            mapping_registry=mapping_registry,
            source=source,
            generated_utc=generated_utc,
        )

        resolution = build_resolution(
            candidates=candidates,
            mapping_registry=mapping_registry,
            generated_utc=generated_utc,
        )

        audit = build_audit(
            resolution=resolution,
            generated_utc=generated_utc,
        )

        manual_review = build_manual_review(
            candidates=candidates,
            resolution=resolution,
        )

        summary = build_summary(
            source=source,
            market_universe=market_universe,
            candidates=candidates,
            resolution=resolution,
            generated_utc=generated_utc,
        )

        validate_outputs(
            candidates=candidates,
            resolution=resolution,
            audit=audit,
        )

        save_sqlite_tables(
            connection=connection,
            candidates=candidates,
            resolution=resolution,
            audit=audit,
            manual_review=manual_review,
            summary=summary,
        )

        connection.commit()

    candidate_paths = save_dataframe_outputs(
        df=candidates,
        output_dir=output_dir,
        filename_stem="cftc_currency_mapping_candidates",
        timestamp=timestamp,
    )

    resolution_paths = save_dataframe_outputs(
        df=resolution,
        output_dir=output_dir,
        filename_stem="cftc_currency_mapping_resolution",
        timestamp=timestamp,
    )

    audit_paths = save_dataframe_outputs(
        df=audit,
        output_dir=output_dir,
        filename_stem="cftc_currency_mapping_audit",
        timestamp=timestamp,
    )

    manual_paths = save_dataframe_outputs(
        df=manual_review,
        output_dir=output_dir,
        filename_stem="cftc_currency_mapping_manual_review",
        timestamp=timestamp,
    )

    save_dataframe_outputs(
        df=summary,
        output_dir=output_dir,
        filename_stem="cftc_currency_mapping_resolution_summary",
        timestamp=timestamp,
    )

    candidates.to_csv(
        REPORT_CANDIDATES_CSV,
        index=False,
    )

    audit.to_csv(
        REPORT_AUDIT_CSV,
        index=False,
    )

    write_reports(
        source=source,
        discovered_sources=discovered_sources,
        market_universe=market_universe,
        resolution=resolution,
        manual_review=manual_review,
        summary=summary,
        database_path=database_path,
        output_dir=output_dir,
        generated_utc=generated_utc,
    )

    summary_row = summary.iloc[0]

    print()
    print("=" * 112)
    print(
        "BACQE CFTC CURRENCY MAPPING "
        "RESOLUTION COMPLETE"
    )
    print("=" * 112)
    print(
        f"Selected source:      "
        f"{source.object_name}"
    )
    print(
        f"Source rows:          "
        f"{source.row_count}"
    )
    print(
        f"Distinct markets:     "
        f"{len(market_universe)}"
    )
    print(
        f"Candidate rows:       "
        f"{len(candidates)}"
    )
    print(
        f"Resolved exact:       "
        f"{int(summary_row['resolved_exact'])}"
    )
    print(
        f"Resolved alias:       "
        f"{int(summary_row['resolved_alias'])}"
    )
    print(
        f"Manual review:        "
        f"{int(summary_row['manual_review_required'])}"
    )
    print(
        f"No comparable:        "
        f"{int(summary_row['no_comparable_contract'])}"
    )
    print(
        f"Safe mapping changes: "
        f"{int(summary_row['safe_mapping_changes'])}"
    )

    print()
    print("Currency resolution:")
    print(
        resolution[
            [
                "currency_code",
                "existing_match_term",
                "recommended_market_name",
                "top_candidate_score",
                "score_margin",
                "resolution_status",
                "mapping_change_required",
            ]
        ].to_string(
            index=False
        )
    )

    if not manual_review.empty:
        print()
        print("Manual review queue:")
        print(
            manual_review[
                [
                    "currency_code",
                    "candidate_rank",
                    "candidate_market_name",
                    "candidate_contract_identity",
                    "candidate_contract_code",
                    "candidate_score",
                    "best_alias",
                ]
            ]
            .head(20)
            .to_string(
                index=False
            )
        )

    print()
    print(
        f"Candidates CSV:  "
        f"{candidate_paths[2]}"
    )
    print(
        f"Resolution CSV:  "
        f"{resolution_paths[2]}"
    )
    print(
        f"Audit CSV:       "
        f"{audit_paths[2]}"
    )
    print(
        f"Manual CSV:      "
        f"{manual_paths[2]}"
    )
    print(
        f"Report TXT:      "
        f"{REPORT_TXT}"
    )
    print()
    print("Overall: PASS")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())