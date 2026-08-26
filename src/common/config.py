"""Load pipeline_config.yaml once and hand out typed sub-sections.

Centralizing config removes the hard-coded paths/table names that were
scattered across the job scripts, so environment differences live in one file.
"""

from __future__ import annotations

import os
from functools import cache

import yaml

_DEFAULT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "config",
    "pipeline_config.yaml",
)


@cache
def load_config(path: str | None = None) -> dict:
    with open(path or os.environ.get("PIPELINE_CONFIG", _DEFAULT_PATH)) as fh:
        return yaml.safe_load(fh)


def dq_rules(dataset: str, path: str | None = None) -> dict:
    """Return the data_quality rules for a dataset (empty dict if none)."""
    return (load_config(path).get("data_quality") or {}).get(dataset, {})


def optimization(path: str | None = None) -> dict:
    return load_config(path).get("optimization", {})
