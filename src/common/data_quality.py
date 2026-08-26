"""
Config-driven data-quality validation framework.

Design goals
------------
* **Single pass.** All row-level rules are compiled into one set of columns
  and evaluated in a single Spark job, instead of the previous pattern of one
  ``df.filter(...).count()`` action per rule (each of which forced a full,
  separate scan). Row-level validation adds an ``_dq_errors`` array column and
  splits valid/quarantined rows without re-scanning.
* **Three failure modes**, chosen per dataset:
    - ``fail``      -> raise on any violation (stop the pipeline).
    - ``warn``      -> log violations, let all rows through.
    - ``quarantine``-> route violating rows to a side table, pass the rest.
* **Config-first.** Rules can be declared in ``pipeline_config.yaml`` and built
  with :func:`from_config`, so analysts can tune thresholds without code changes.

Supported checks
----------------
Row-level (evaluated per record):
  not_null, in_range (min/max, inclusive), allowed_values, matches_regex,
  non_negative (convenience wrapper over in_range).
Dataset-level (evaluated over the batch):
  unique (primary/business key), min_row_count, freshness (max timestamp not
  older than N hours), referential integrity via :meth:`foreign_key`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class OnFailure(str, Enum):
    FAIL = "fail"
    WARN = "warn"
    QUARANTINE = "quarantine"


@dataclass
class CheckResult:
    check: str
    passed: bool
    violations: int
    detail: str = ""


@dataclass
class ValidationOutcome:
    dataset: str
    results: list[CheckResult]
    valid: DataFrame
    quarantined: DataFrame | None
    total_rows: int
    quarantine_rows: int = 0

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def valid_rows(self) -> int:
        return self.total_rows - self.quarantine_rows

    def summary(self) -> str:
        lines = [
            f"[DQ] {self.dataset}: {'PASS' if self.passed else 'FAIL'} "
            f"({self.total_rows} rows checked)"
        ]
        for r in self.results:
            status = "ok " if r.passed else "FAIL"
            lines.append(f"  - {status} {r.check}: {r.violations} violation(s) {r.detail}")
        return "\n".join(lines)


class DataQualityValidator:
    """Accumulate rules, then :meth:`validate` a DataFrame in a single pass."""

    def __init__(self, dataset: str, on_failure: OnFailure = OnFailure.FAIL):
        self.dataset = dataset
        self.on_failure = OnFailure(on_failure)
        # Row-level rules: (name, condition_that_is_true_when_row_is_VALID)
        self._row_rules: list[tuple[str, F.Column]] = []
        # Dataset-level rules: (name, callable(df) -> CheckResult)
        self._dataset_rules: list = []
        self._total: int = 0  # populated by validate() before dataset rules run

    # ---- row-level rule builders -----------------------------------------
    def not_null(self, *cols: str) -> DataQualityValidator:
        for c in cols:
            self._row_rules.append((f"not_null[{c}]", F.col(c).isNotNull()))
        return self

    def in_range(self, colname: str, min=None, max=None) -> DataQualityValidator:
        cond = F.lit(True)
        if min is not None:
            cond = cond & (F.col(colname) >= F.lit(min))
        if max is not None:
            cond = cond & (F.col(colname) <= F.lit(max))
        # NULLs are not a range violation here (handled by not_null); treat them as valid.
        cond = F.col(colname).isNull() | cond
        self._row_rules.append((f"in_range[{colname}]", cond))
        return self

    def non_negative(self, colname: str) -> DataQualityValidator:
        return self.in_range(colname, min=0)

    def allowed_values(self, colname: str, values) -> DataQualityValidator:
        allowed = [v for v in values]
        cond = F.col(colname).isNull() | F.col(colname).isin(allowed)
        self._row_rules.append((f"allowed_values[{colname}]", cond))
        return self

    def matches_regex(self, colname: str, pattern: str) -> DataQualityValidator:
        cond = F.col(colname).isNull() | (F.col(colname).rlike(pattern))
        self._row_rules.append((f"matches_regex[{colname}]", cond))
        return self

    # ---- dataset-level rule builders -------------------------------------
    def unique(self, *keys: str) -> DataQualityValidator:
        key_cols = list(keys)

        def _check(df: DataFrame) -> CheckResult:
            dupes = df.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
            return CheckResult(
                check=f"unique[{','.join(key_cols)}]",
                passed=dupes == 0,
                violations=dupes,
                detail="duplicate key group(s)" if dupes else "",
            )

        self._dataset_rules.append(_check)
        return self

    def min_row_count(self, minimum: int) -> DataQualityValidator:
        def _check(_df: DataFrame) -> CheckResult:
            # Reuses the single row count computed once in validate().
            n = self._total
            return CheckResult(
                check=f"min_row_count[>= {minimum}]",
                passed=n >= minimum,
                violations=max(0, minimum - n),
                detail=f"got {n}",
            )

        self._dataset_rules.append(_check)
        return self

    def freshness(self, ts_col: str, max_age_hours: float) -> DataQualityValidator:
        def _check(df: DataFrame) -> CheckResult:
            row = df.select(F.max(ts_col).alias("mx")).collect()[0]
            newest = row["mx"]
            if newest is None:
                return CheckResult(f"freshness[{ts_col}]", False, 1, "no timestamps")
            age_h = df.select(
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.lit(newest))) / 3600.0
            ).collect()[0][0]
            return CheckResult(
                check=f"freshness[{ts_col} <= {max_age_hours}h]",
                passed=age_h <= max_age_hours,
                violations=0 if age_h <= max_age_hours else 1,
                detail=f"newest is {age_h:.1f}h old",
            )

        self._dataset_rules.append(_check)
        return self

    def foreign_key(self, colname: str, ref_df: DataFrame, ref_col: str) -> DataQualityValidator:
        """Referential-integrity check: every non-null ``colname`` must exist in
        ``ref_df.ref_col``. Uses a broadcast anti-join for efficiency."""
        ref_keys = ref_df.select(F.col(ref_col).alias("_fk")).distinct()

        def _check(df: DataFrame) -> CheckResult:
            orphans = (
                df.filter(F.col(colname).isNotNull())
                .join(F.broadcast(ref_keys), df[colname] == ref_keys["_fk"], "left_anti")
                .count()
            )
            return CheckResult(
                check=f"foreign_key[{colname} -> {ref_col}]",
                passed=orphans == 0,
                violations=orphans,
                detail="orphan row(s)" if orphans else "",
            )

        self._dataset_rules.append(_check)
        return self

    # ---- execution --------------------------------------------------------
    def validate(self, df: DataFrame) -> ValidationOutcome:
        # Cache: multiple dataset-level checks + the split would otherwise
        # recompute the (possibly expensive) upstream DAG repeatedly.
        df = df.cache()
        total = df.count()
        self._total = total

        results: list[CheckResult] = []
        quarantined: DataFrame | None = None
        valid = df

        # ---- row-level: one pass, collect per-row error names -------------
        if self._row_rules:
            # One column per rule: the rule name when the row FAILS it, else null.
            # Then drop the nulls with the higher-order `filter` (array_remove does
            # not remove nulls in Spark).
            tagged_arr = F.array(
                *[
                    F.when(~cond, F.lit(name)).otherwise(F.lit(None).cast("string"))
                    for name, cond in self._row_rules
                ]
            )
            err = F.filter(tagged_arr, lambda x: x.isNotNull())
            tagged = df.withColumn("_dq_errors", err).cache()

            # Per-rule violation counts from a single aggregation (all modes).
            exploded = tagged.select(F.explode("_dq_errors").alias("rule"))
            counts = {
                r["rule"]: r["cnt"]
                for r in exploded.groupBy("rule").agg(F.count("*").alias("cnt")).collect()
            }
            for name, _ in self._row_rules:
                v = counts.get(name, 0)
                results.append(CheckResult(check=name, passed=v == 0, violations=v))

            # Only quarantine mode removes bad rows from the published output.
            # warn/fail let all rows through here (fail raises in _enforce below).
            if self.on_failure == OnFailure.QUARANTINE:
                valid = tagged.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
                quarantined = (
                    tagged.filter(F.size("_dq_errors") > 0)
                    .withColumn("_dq_dataset", F.lit(self.dataset))
                    .withColumn("_dq_ts", F.current_timestamp())
                )

        # ---- dataset-level ------------------------------------------------
        for rule in self._dataset_rules:
            results.append(rule(df))

        outcome = ValidationOutcome(
            dataset=self.dataset,
            results=results,
            valid=valid,
            quarantined=quarantined,
            total_rows=total,
        )
        # store quarantine count for reporting without an extra full scan when possible
        outcome.quarantine_rows = 0 if quarantined is None else quarantined.count()

        print(outcome.summary())
        self._enforce(outcome)
        return outcome

    def _enforce(self, outcome: ValidationOutcome):
        if outcome.passed:
            return
        if self.on_failure == OnFailure.FAIL:
            failed = [r.check for r in outcome.results if not r.passed]
            raise DataQualityError(
                f"Data quality FAILED for '{self.dataset}': {', '.join(failed)}\n"
                + outcome.summary()
            )
        # WARN / QUARANTINE: violations already logged via summary(); continue.


class DataQualityError(Exception):
    """Raised when a dataset fails validation under ``on_failure='fail'``."""


def write_quarantine(outcome: ValidationOutcome, table: str):
    """Persist quarantined rows to a Delta side table for later inspection."""
    if outcome.quarantined is None or outcome.quarantine_rows == 0:
        return
    (
        outcome.quarantined.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )
    print(f"[DQ] Quarantined {outcome.quarantine_rows} row(s) to {table}")


def from_config(dataset: str, cfg: dict) -> DataQualityValidator:
    """Build a validator from a config dict, e.g. the ``data_quality.<dataset>``
    block of pipeline_config.yaml.

    Example config::

        orders:
          on_failure: quarantine
          not_null: [order_id, customer_id, order_date]
          unique: [order_id]
          ranges:
            amount: { min: 0 }
          allowed_values:
            order_status: [pending, shipped, delivered, cancelled, returned]
          min_row_count: 1
    """
    v = DataQualityValidator(dataset, on_failure=cfg.get("on_failure", "fail"))
    if cfg.get("not_null"):
        v.not_null(*cfg["not_null"])
    for c in cfg.get("non_negative", []):
        v.non_negative(c)
    for colname, bounds in (cfg.get("ranges") or {}).items():
        v.in_range(colname, min=bounds.get("min"), max=bounds.get("max"))
    for colname, values in (cfg.get("allowed_values") or {}).items():
        v.allowed_values(colname, values)
    for colname, pattern in (cfg.get("regex") or {}).items():
        v.matches_regex(colname, pattern)
    if cfg.get("unique"):
        v.unique(*cfg["unique"])
    if cfg.get("min_row_count") is not None:
        v.min_row_count(int(cfg["min_row_count"]))
    fresh = cfg.get("freshness")
    if fresh:
        v.freshness(fresh["column"], float(fresh["max_age_hours"]))
    return v
