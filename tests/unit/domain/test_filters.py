"""Filter AST validation and reference-evaluator semantics."""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import TypeAdapter
from pydantic import ValidationError as PydanticValidationError

from retrievalloom.domain.filters import (
    And,
    Eq,
    Exists,
    FilterExpression,
    In,
    Not,
    Or,
    Range,
    combine,
    evaluate,
    measure,
    resolve_field,
)

ADAPTER: TypeAdapter[Any] = TypeAdapter(FilterExpression)

RECORD: dict[str, Any] = {
    "lang": "en",
    "year": 2020,
    "score": 0.5,
    "flag": True,
    "none": None,
    "tags": ["a", "b"],
    "nested": {"inner": {"leaf": 3}},
}

scalars = st.none() | st.booleans() | st.integers(-5, 5) | st.sampled_from(["a", "b", "c"])
fields = st.sampled_from(["f", "g", "h.x"])
records = st.fixed_dictionaries(
    {},
    optional={
        "f": scalars | st.lists(scalars, max_size=3),
        "g": scalars,
        "h": st.fixed_dictionaries({}, optional={"x": scalars}) | scalars,
    },
)
leaves = st.one_of(
    st.builds(Eq, field=fields, value=scalars),
    st.builds(In, field=fields, values=st.lists(scalars, min_size=1, max_size=3).map(tuple)),
    st.builds(Exists, field=fields),
    st.builds(Range, field=fields, gte=st.integers(-5, 5), lt=st.integers(-5, 5)),
)
expressions = st.recursive(
    leaves,
    lambda children: st.one_of(
        st.builds(And, clauses=st.lists(children, min_size=1, max_size=3).map(tuple)),
        st.builds(Or, clauses=st.lists(children, min_size=1, max_size=3).map(tuple)),
        st.builds(Not, clause=children),
    ),
    max_leaves=6,
)


# --- validation ------------------------------------------------------------------------


def test_discriminated_union_parses_nested_json() -> None:
    expression = ADAPTER.validate_python(
        {
            "op": "and",
            "clauses": [
                {"op": "eq", "field": "lang", "value": "en"},
                {"op": "not", "clause": {"op": "exists", "field": "none"}},
                {"op": "or", "clauses": [{"op": "in", "field": "tags", "values": ["z", "b"]}]},
                {"op": "range", "field": "year", "gte": 2000},
            ],
        }
    )
    assert isinstance(expression, And)
    assert isinstance(expression.clauses[1], Not)
    assert evaluate(expression, RECORD)
    assert ADAPTER.validate_json(ADAPTER.dump_json(expression)) == expression


@pytest.mark.parametrize(
    "payload",
    [
        {"op": "range", "field": "x"},
        {"op": "in", "field": "x", "values": []},
        {"op": "and", "clauses": []},
        {"op": "or", "clauses": []},
        {"op": "eq", "field": "", "value": 1},
        {"op": "eq", "field": "x" * 257, "value": 1},
        {"op": "bogus", "field": "x"},
        {"op": "eq", "field": "x", "value": 1, "extra": True},
    ],
)
def test_invalid_expressions_rejected(payload: dict[str, Any]) -> None:
    with pytest.raises(PydanticValidationError):
        ADAPTER.validate_python(payload)


def test_expressions_are_frozen() -> None:
    expression = Eq(field="a", value=1)
    with pytest.raises(PydanticValidationError):
        expression.value = 2  # type: ignore[misc]


# --- evaluation ------------------------------------------------------------------------


def test_none_matches_everything() -> None:
    assert evaluate(None, {})


def test_resolve_field_dotted_paths() -> None:
    assert resolve_field(RECORD, "nested.inner.leaf") == 3
    missing = resolve_field(RECORD, "nested.absent")
    assert resolve_field(RECORD, "lang.deeper") is missing
    assert resolve_field({}, "x") is missing


def test_eq_semantics() -> None:
    assert evaluate(Eq(field="lang", value="en"), RECORD)
    assert evaluate(Eq(field="nested.inner.leaf", value=3), RECORD)
    assert not evaluate(Eq(field="lang", value="de"), RECORD)
    assert not evaluate(Eq(field="absent", value=None), RECORD)
    assert evaluate(Eq(field="none", value=None), RECORD)
    # bool and int never compare equal even though True == 1 in Python
    assert not evaluate(Eq(field="flag", value=1), RECORD)
    assert not evaluate(Eq(field="year", value=True), {"year": 1})
    assert evaluate(Eq(field="flag", value=True), RECORD)
    # list field matches when any element equals the scalar
    assert evaluate(Eq(field="tags", value="b"), RECORD)
    assert not evaluate(Eq(field="tags", value="z"), RECORD)
    # list compared with list is whole-value equality
    assert evaluate(Eq(field="tags", value=["a", "b"]), RECORD)
    assert not evaluate(Eq(field="tags", value=["b"]), RECORD)


def test_in_semantics() -> None:
    assert evaluate(In(field="lang", values=("de", "en")), RECORD)
    assert not evaluate(In(field="lang", values=("de",)), RECORD)
    assert evaluate(In(field="tags", values=("x", "a")), RECORD)
    assert not evaluate(In(field="tags", values=("x", "y")), RECORD)
    assert not evaluate(In(field="absent", values=(None,)), RECORD)
    assert not evaluate(In(field="flag", values=(1,)), RECORD)


def test_exists_semantics() -> None:
    assert evaluate(Exists(field="lang"), RECORD)
    assert evaluate(Exists(field="flag"), {"flag": False})
    assert not evaluate(Exists(field="none"), RECORD)
    assert not evaluate(Exists(field="absent"), RECORD)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (Range(field="year", gte=2020), True),
        (Range(field="year", gt=2020), False),
        (Range(field="year", lte=2020, gt=2019.5), True),
        (Range(field="year", lt=2020), False),
        (Range(field="score", gt=0, lt=1), True),
        (Range(field="lang", gte="a", lt="f"), True),
        (Range(field="lang", gt="en"), False),
        # type mismatches never match
        (Range(field="lang", gte=0), False),
        (Range(field="year", gte="2000"), False),
        (Range(field="flag", gte=0), False),
        (Range(field="tags", gte=0), False),
        (Range(field="none", gte=0), False),
        (Range(field="absent", gte=0), False),
    ],
)
def test_range_semantics(expression: Range, expected: bool) -> None:
    assert evaluate(expression, RECORD) is expected


def test_range_bool_bound_never_matches() -> None:
    # A bool bound cannot come from validation (it coerces to float), so construct directly.
    expression = Range.model_construct(field="x", op="range", gte=True, gt=None, lt=None, lte=None)
    assert not evaluate(expression, {"x": 1})


def test_boolean_combinators() -> None:
    yes = Eq(field="lang", value="en")
    no = Eq(field="lang", value="de")
    assert evaluate(And(clauses=(yes, yes)), RECORD)
    assert not evaluate(And(clauses=(yes, no)), RECORD)
    assert evaluate(Or(clauses=(no, yes)), RECORD)
    assert not evaluate(Or(clauses=(no, no)), RECORD)
    assert evaluate(Not(clause=no), RECORD)
    # negation of a predicate on a missing field matches
    assert evaluate(Not(clause=Eq(field="absent", value=1)), RECORD)


@settings(max_examples=150)
@given(expressions, records)
def test_combinator_laws(expression: Any, record: dict[str, Any]) -> None:
    result = evaluate(expression, record)
    assert evaluate(Not(clause=expression), record) is (not result)
    assert evaluate(Not(clause=Not(clause=expression)), record) is result
    assert evaluate(And(clauses=(expression,)), record) is result
    assert evaluate(Or(clauses=(expression,)), record) is result
    assert evaluate(combine(None, expression, None), record) is result
    roundtrip = ADAPTER.validate_json(ADAPTER.dump_json(expression))
    assert evaluate(roundtrip, record) is result


@settings(max_examples=100)
@given(st.lists(expressions, min_size=1, max_size=3), records)
def test_and_or_match_all_any(clauses: list[Any], record: dict[str, Any]) -> None:
    results = [evaluate(clause, record) for clause in clauses]
    assert evaluate(And(clauses=tuple(clauses)), record) is all(results)
    assert evaluate(Or(clauses=tuple(clauses)), record) is any(results)
    assert evaluate(combine(*clauses), record) is all(results)


@settings(max_examples=100)
@given(fields, scalars, records)
def test_single_value_in_equals_eq(field: str, value: Any, record: dict[str, Any]) -> None:
    assert evaluate(In(field=field, values=(value,)), record) is evaluate(
        Eq(field=field, value=value), record
    )


@settings(max_examples=100)
@given(st.integers(-10, 10), st.integers(-10, 10), st.integers(-10, 10))
def test_numeric_range_matches_python_comparison(value: int, low: int, high: int) -> None:
    expression = Range(field="v", gte=low, lt=high)
    assert evaluate(expression, {"v": value}) is (low <= value < high)


# --- measure / combine ------------------------------------------------------------------


def test_measure_depth_and_size() -> None:
    leaf = Eq(field="a", value=1)
    assert measure(None) == (0, 0)
    assert measure(leaf) == (1, 1)
    assert measure(Not(clause=leaf)) == (2, 2)
    tree = And(clauses=(leaf, Or(clauses=(leaf, Not(clause=leaf)))))
    assert measure(tree) == (4, 6)


def test_combine() -> None:
    leaf = Eq(field="a", value=1)
    other = Exists(field="b")
    assert combine() is None
    assert combine(None, None) is None
    assert combine(None, leaf) is leaf
    assert combine(leaf, None, other) == And(clauses=(leaf, other))
