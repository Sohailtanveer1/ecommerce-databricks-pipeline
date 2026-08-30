"""Tests for the Gold model config + pure helpers. The SCD2/fact Spark logic
runs in CI; here we lock the model shape and naming (surrogate/natural keys)."""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src", "gold")))

import gold_generic  # noqa: E402

MODEL = os.path.join(os.path.dirname(__file__), "..", "config", "gold_model.yaml")


def test_entity_strips_dim_prefix():
    assert gold_generic._entity("dim_customer") == "customer"
    assert gold_generic._entity("dim_product") == "product"
    assert gold_generic._entity("customer") == "customer"


def test_model_loads_and_is_well_formed():
    m = gold_generic.load_model(MODEL)
    dims = {d["name"] for d in m["dimensions"]}
    facts = {f["name"] for f in m["facts"]}
    assert {"dim_customer", "dim_product"} <= dims
    assert "fact_orders" in facts

    # every dimension declares a natural key + tracked SCD2 attributes
    for d in m["dimensions"]:
        assert d["natural_key"] and d["scd2"] and d["attributes"]

    # the fact resolves its dimensions and has a grain + event_date for PIT lookup
    fo = next(f for f in m["facts"] if f["name"] == "fact_orders")
    assert fo["grain"] == ["order_id"]
    assert fo["event_date"] == "order_date"
    assert any(dr["dim"] == "dim_customer" for dr in fo["dimensions"])


def test_surrogate_and_natural_key_naming():
    # dim_customer -> surrogate customer_sk (bigint by convention) + natural customer_id
    from framework.standardize import infer_type

    entity = gold_generic._entity("dim_customer")
    assert infer_type(f"{entity}_sk") == "bigint"
    assert infer_type(f"{entity}_id") == "string"
