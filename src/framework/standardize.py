"""
Standardization layer — enforce naming + type conventions on the way into Silver,
so every table across every source looks the same. Conventions are enforced by
code, not discipline (see docs/NAMING_CONVENTIONS.md for the human-readable spec).

Applied per object in silver_generic, in order:
  1. explicit `rename` map (source spelling -> canonical business name)
  2. snake_case every remaining column (CustomerID/order-amt -> customer_id/order_amt)
  3. trim string columns (leading/trailing whitespace is a top data-quality gremlin)
  4. type coercion: explicit `cast` overrides first, then **convention by suffix/
     prefix** — a column's name determines its type, so types are consistent by
     construction (an `*_amount` is DECIMAL(18,2) everywhere, `*_date` is DATE, ...)

Bad values that can't cast become NULL and are then caught by the DQ/quarantine
step (which runs after standardization), so a mistyped value is quarantined, not
silently kept.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Convention: a column's SUFFIX determines its canonical type.
SUFFIX_TYPES = {
    "_id": "string",  # natural/business keys are strings (never lose leading zeros)
    "_sk": "bigint",  # surrogate keys
    "_date": "date",
    "_at": "timestamp",  # *_at / *_ts / *_timestamp -> TIMESTAMP (store UTC)
    "_ts": "timestamp",
    "_timestamp": "timestamp",
    "_amount": "decimal(18,2)",  # money
    "_price": "decimal(18,2)",
    "_cost": "decimal(18,2)",
    "_rate": "decimal(18,6)",
    "_pct": "decimal(9,4)",
    "_qty": "int",
    "_quantity": "int",
    "_count": "bigint",
}
# Convention: a column's PREFIX can determine its type too.
PREFIX_TYPES = {"is_": "boolean", "has_": "boolean"}

# Metadata columns are exempt from renaming/typing.
_EXEMPT_PREFIXES = ("_dq_", "_rescued", "_metadata")


def snake_case(name: str) -> str:
    """CamelCase / spaces / dots / dashes -> snake_case. 'CustomerID' -> 'customer_id'."""
    s = name.strip()
    s = re.sub(r"[\s./\-]+", "_", s)
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)  # camelCase boundary
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)  # ABBRWord -> ABBR_Word
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


def infer_type(col: str) -> str | None:
    for p, t in PREFIX_TYPES.items():
        if col.startswith(p):
            return t
    for suf, t in SUFFIX_TYPES.items():
        if col.endswith(suf):
            return t
    return None


def _exempt(col: str) -> bool:
    return any(col.startswith(p) for p in _EXEMPT_PREFIXES)


def standardize(
    df: DataFrame,
    *,
    rename: dict | None = None,
    cast: dict | None = None,
    trim_strings: bool = True,
    apply_conventions: bool = True,
) -> DataFrame:
    # 1) explicit semantic renames (source spelling -> canonical name)
    for src, tgt in (rename or {}).items():
        if src in df.columns and src != tgt:
            df = df.withColumnRenamed(src, tgt)

    # 2) snake_case everything else
    if apply_conventions:
        for c in list(df.columns):
            if _exempt(c):
                continue
            sc = snake_case(c)
            if sc != c and sc not in df.columns:
                df = df.withColumnRenamed(c, sc)

    # 3) trim string columns
    if trim_strings:
        for f in df.schema.fields:
            if isinstance(f.dataType, StringType) and not _exempt(f.name):
                df = df.withColumn(f.name, F.trim(F.col(f.name)))

    # 4) type coercion: explicit casts first, then convention-by-name
    casts = dict(cast or {})
    if apply_conventions:
        for c in df.columns:
            if _exempt(c) or c in casts:
                continue
            t = infer_type(c)
            if t:
                casts[c] = t
    for c, t in casts.items():
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(t))

    return df
