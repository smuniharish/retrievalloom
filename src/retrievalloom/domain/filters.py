"""Backend-neutral metadata filter AST and its reference evaluator."""

from __future__ import annotations

import operator
from collections.abc import Callable, Mapping
from typing import Annotated, Any, Literal

from pydantic import Field, JsonValue, model_validator

from retrievalloom.domain.base import FrozenModel

_MISSING = object()
_COMPARATORS: dict[str, Callable[[Any, Any], Any]] = {
    "gt": operator.gt,
    "gte": operator.ge,
    "lt": operator.lt,
    "lte": operator.le,
}


class _Predicate(FrozenModel):
    field: str = Field(min_length=1, max_length=256)


class Eq(_Predicate):
    """``field == value``."""

    op: Literal["eq"] = "eq"
    value: JsonValue


class In(_Predicate):
    """``field in values``."""

    op: Literal["in"] = "in"
    values: tuple[JsonValue, ...] = Field(min_length=1)


class Range(_Predicate):
    """Numeric or string range over ``field``."""

    op: Literal["range"] = "range"
    gt: float | str | None = None
    gte: float | str | None = None
    lt: float | str | None = None
    lte: float | str | None = None

    @model_validator(mode="after")
    def _check_bounds(self) -> Range:
        if all(bound is None for bound in (self.gt, self.gte, self.lt, self.lte)):
            raise ValueError("range filter needs at least one bound")
        return self


class Exists(_Predicate):
    """``field`` is present (and not null)."""

    op: Literal["exists"] = "exists"


class And(FrozenModel):
    """All clauses match."""

    op: Literal["and"] = "and"
    clauses: tuple[FilterExpression, ...] = Field(min_length=1)


class Or(FrozenModel):
    """At least one clause matches."""

    op: Literal["or"] = "or"
    clauses: tuple[FilterExpression, ...] = Field(min_length=1)


class Not(FrozenModel):
    """The clause does not match."""

    op: Literal["not"] = "not"
    clause: FilterExpression


FilterExpression = Annotated[Eq | In | Range | Exists | And | Or | Not, Field(discriminator="op")]

And.model_rebuild()
Or.model_rebuild()
Not.model_rebuild()


def resolve_field(record: Mapping[str, Any], field: str) -> Any:
    """Resolve a dotted ``field`` path; returns a private sentinel when absent."""
    current: Any = record
    for part in field.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _compare(value: Any, bound: Any, operation: str) -> bool:
    if isinstance(value, bool) or isinstance(bound, bool):
        return False
    numeric = isinstance(value, int | float) and isinstance(bound, int | float)
    textual = isinstance(value, str) and isinstance(bound, str)
    if not (numeric or textual):
        return False
    return bool(_COMPARATORS[operation](value, bound))


def evaluate(expression: FilterExpression | None, record: Mapping[str, Any]) -> bool:
    """Evaluate ``expression`` against a flat or nested metadata mapping.

    Missing fields never match a positive predicate. ``None`` matches everything.
    """
    if expression is None:
        return True
    if isinstance(expression, And):
        return all(evaluate(clause, record) for clause in expression.clauses)
    if isinstance(expression, Or):
        return any(evaluate(clause, record) for clause in expression.clauses)
    if isinstance(expression, Not):
        return not evaluate(expression.clause, record)
    value = resolve_field(record, expression.field)
    if isinstance(expression, Exists):
        return value is not _MISSING and value is not None
    if value is _MISSING:
        return False
    if isinstance(expression, Eq):
        return _values_equal(value, expression.value)
    if isinstance(expression, In):
        if isinstance(value, list):
            return any(
                _values_equal(item, option) for item in value for option in expression.values
            )
        return any(_values_equal(value, option) for option in expression.values)
    bounds = {
        "gt": expression.gt,
        "gte": expression.gte,
        "lt": expression.lt,
        "lte": expression.lte,
    }
    return all(
        _compare(value, bound, operation)
        for operation, bound in bounds.items()
        if bound is not None
    )


def _values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, list) and not isinstance(right, list):
        return any(_values_equal(item, right) for item in left)
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    return left == right


def measure(expression: FilterExpression | None) -> tuple[int, int]:
    """Return ``(depth, node_count)`` of a filter expression."""
    if expression is None:
        return (0, 0)
    if isinstance(expression, And | Or):
        children = [measure(clause) for clause in expression.clauses]
        return (1 + max(depth for depth, _ in children), 1 + sum(size for _, size in children))
    if isinstance(expression, Not):
        depth, size = measure(expression.clause)
        return (depth + 1, size + 1)
    return (1, 1)


def combine(*expressions: FilterExpression | None) -> FilterExpression | None:
    """AND together the non-null expressions."""
    present = [expression for expression in expressions if expression is not None]
    if not present:
        return None
    if len(present) == 1:
        return present[0]
    return And(clauses=tuple(present))
