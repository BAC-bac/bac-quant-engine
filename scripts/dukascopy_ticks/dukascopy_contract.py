"""Authoritative BACQE contract for Dukascopy tick normalisation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any


SYMBOL_METADATA_SCHEMA_VERSION = "dukascopy_symbol_metadata_v1"
NORMALISATION_SCHEMA_VERSION = "dukascopy_normalised_ticks_v2"
CONTRACT_PROVENANCE_URL = "https://www.dukascopy.com/wiki/en/development/data-export/"


class DukascopyContractError(ValueError):
    """Raised when a symbol or normalised file is outside the approved contract."""


@dataclass(frozen=True)
class DukascopySymbolMetadata:
    symbol: str
    asset_class: str
    base_currency: str
    quote_currency: str
    raw_price_scale: int
    point_size: float
    pip_size: float
    display_decimal_precision: int
    metadata_schema_version: str
    certification_status: str
    verification_note: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _fx(
    symbol: str,
    raw_price_scale: int,
    point_size: float,
    pip_size: float,
    precision: int,
    fixture_note: str,
) -> DukascopySymbolMetadata:
    return DukascopySymbolMetadata(
        symbol=symbol,
        asset_class="fx_spot",
        base_currency=symbol[:3],
        quote_currency=symbol[3:],
        raw_price_scale=raw_price_scale,
        point_size=point_size,
        pip_size=pip_size,
        display_decimal_precision=precision,
        metadata_schema_version=SYMBOL_METADATA_SCHEMA_VERSION,
        certification_status="certified",
        verification_note=(
            "Dukascopy BI5 export documentation specifies 100000 for standard FX "
            "and 1000 for JPY pairs; checked against repository raw fixture "
            f"{fixture_note}."
        ),
    )


SYMBOL_REGISTRY: dict[str, DukascopySymbolMetadata] = {
    "EURUSD": _fx("EURUSD", 100_000, 0.00001, 0.0001, 5, "ask_raw=110370 -> 1.10370"),
    "GBPUSD": _fx("GBPUSD", 100_000, 0.00001, 0.0001, 5, "ask_raw=121091 -> 1.21091"),
    "EURGBP": _fx("EURGBP", 100_000, 0.00001, 0.0001, 5, "ask_raw=85436 -> 0.85436"),
    "USDJPY": _fx("USDJPY", 1_000, 0.001, 0.01, 3, "ask_raw=131001 -> 131.001"),
    "EURJPY": _fx("EURJPY", 1_000, 0.001, 0.01, 3, "ask_raw=140339 -> 140.339"),
    "GBPJPY": _fx("GBPJPY", 1_000, 0.001, 0.01, 3, "ask_raw=144570 -> 144.570"),
}


def certified_symbols() -> tuple[str, ...]:
    return tuple(sorted(SYMBOL_REGISTRY))


def get_symbol_metadata(symbol: str) -> DukascopySymbolMetadata:
    normalized = str(symbol).upper().strip()
    metadata = SYMBOL_REGISTRY.get(normalized)
    if metadata is None:
        raise DukascopyContractError(
            f"Unsupported Dukascopy symbol {normalized!r}; certified symbols: "
            f"{list(certified_symbols())}"
        )
    if metadata.certification_status != "certified":
        raise DukascopyContractError(
            f"Dukascopy symbol {normalized!r} is not certified: "
            f"{metadata.certification_status}"
        )
    if metadata.metadata_schema_version != SYMBOL_METADATA_SCHEMA_VERSION:
        raise DukascopyContractError(
            f"Dukascopy symbol {normalized!r} has incompatible metadata version"
        )
    return metadata


def registry_payload() -> dict[str, dict[str, Any]]:
    return {symbol: metadata.to_dict() for symbol, metadata in sorted(SYMBOL_REGISTRY.items())}


def registry_fingerprint() -> str:
    payload = json.dumps(registry_payload(), sort_keys=True, separators=(",", ":"))
    return sha256(payload.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_normalised_parquet(frame: Any, path: Path, lineage: dict[str, Any]) -> None:
    """Write a normalised frame with contract metadata in the Parquet footer."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    existing = dict(table.schema.metadata or {})
    footer = {
        "normalisation_schema_version": NORMALISATION_SCHEMA_VERSION,
        "symbol_metadata_schema_version": SYMBOL_METADATA_SCHEMA_VERSION,
        "symbol_registry_fingerprint": registry_fingerprint(),
        **{key: str(value) for key, value in lineage.items()},
    }
    existing.update({str(key).encode(): str(value).encode() for key, value in footer.items()})
    pq.write_table(table.replace_schema_metadata(existing), path)


def read_normalisation_metadata(path: Path) -> dict[str, str]:
    """Read BACQE contract metadata without loading tick columns."""
    import pyarrow.parquet as pq

    raw = pq.read_metadata(path).metadata or {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in raw.items()
        if key != b"ARROW:schema"
    }


def validate_normalised_parquet(path: Path, expected_symbol: str | None = None) -> dict[str, Any]:
    """Return a fail-closed certification result for one processed tick file."""
    try:
        footer = read_normalisation_metadata(path)
    except Exception as exc:
        return {"certified": False, "reason": f"metadata_read_error:{exc}", "metadata": {}}

    required = {
        "normalisation_schema_version": NORMALISATION_SCHEMA_VERSION,
        "symbol_metadata_schema_version": SYMBOL_METADATA_SCHEMA_VERSION,
        "symbol_registry_fingerprint": registry_fingerprint(),
        "coverage_status": "complete_coverage",
    }
    for key, expected in required.items():
        if footer.get(key) != expected:
            return {
                "certified": False,
                "reason": f"{key}={footer.get(key)!r}, expected {expected!r}",
                "metadata": footer,
            }

    symbol = footer.get("symbol", "")
    try:
        get_symbol_metadata(symbol)
    except DukascopyContractError as exc:
        return {"certified": False, "reason": str(exc), "metadata": footer}

    if expected_symbol and symbol != expected_symbol.upper().strip():
        return {
            "certified": False,
            "reason": f"symbol={symbol!r}, expected {expected_symbol.upper().strip()!r}",
            "metadata": footer,
        }
    return {"certified": True, "reason": "certified", "metadata": footer}


def inventory_normalised_symbol(root: Path, symbol: str) -> dict[str, Any]:
    """Inventory legacy and certified daily files by Parquet footer contract."""
    approved = get_symbol_metadata(symbol)
    symbol_root = root / f"symbol={approved.symbol}"
    all_dates: set[str] = set()
    certified_dates: set[str] = set()
    reasons: dict[str, int] = {}
    if not symbol_root.exists():
        return {
            "all_dates": all_dates,
            "certified_dates": certified_dates,
            "uncertified_files": 0,
            "reasons": reasons,
        }
    uncertified = 0
    for path in symbol_root.rglob("*.parquet"):
        match = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
        if not match:
            continue
        date = match.group(1)
        all_dates.add(date)
        result = validate_normalised_parquet(path, expected_symbol=approved.symbol)
        if result["certified"]:
            certified_dates.add(date)
        else:
            uncertified += 1
            reason = str(result["reason"])
            reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "all_dates": all_dates,
        "certified_dates": certified_dates,
        "uncertified_files": uncertified,
        "reasons": reasons,
    }
