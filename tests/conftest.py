"""Shared pytest fixtures + import-path setup for the test suite.

The Spark scripts under src/ are structured as standalone job scripts rather
than an installable package, so we add each layer directory to sys.path here.
This keeps individual test modules free of per-file sys.path boilerplate.
"""

import os
import sys

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
for _layer in ("ingestion", "bronze", "silver", "gold"):
    sys.path.insert(0, os.path.abspath(os.path.join(_SRC, _layer)))


@pytest.fixture(scope="session")
def spark():
    """A local Spark session shared across the whole test session."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
