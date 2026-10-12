from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional

import pandas as pd


# field name -> the column names accepted for it, most specific first
AliasTable = Mapping[str, list[str]]


def normalize_name(value: Any) -> str:
    """Reduce a column name to letters and digits.

    Exports disagree on spelling, not meaning: "Order Date", "order_date" and
    "OrderDate" are the same column, as are "Close/Last" and "close last".
    """
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


@dataclass(frozen=True)
class FieldMatch:
    matched: dict[str, Any]  # field name -> the dataset's own column
    missing: list[str]


def match_aliases(columns: Iterable[Any], aliases: AliasTable) -> FieldMatch:
    by_name: dict[str, Any] = {}
    for column in columns:
        by_name.setdefault(normalize_name(column), column)

    matched: dict[str, Any] = {}
    missing: list[str] = []
    for field_name, names in aliases.items():
        column = next((by_name[key] for key in map(normalize_name, names) if key in by_name), None)
        if column is None:
            missing.append(field_name)
        else:
            matched[field_name] = column
    return FieldMatch(matched=matched, missing=missing)


def detect_columns(
    df: pd.DataFrame,
    required: AliasTable,
    optional: Optional[AliasTable] = None,
) -> Optional[dict[str, dict[str, Any]]]:
    """Map a template's fields onto the dataset's columns, or None if a required field has no column."""
    required_match = match_aliases(df.columns, required)
    if required_match.missing:
        return None
    return {"required": required_match.matched, "optional": match_aliases(df.columns, optional or {}).matched}
