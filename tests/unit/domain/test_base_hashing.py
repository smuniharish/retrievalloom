"""Primitive validators, base models and deterministic hashing."""

from __future__ import annotations

import json
import re
from datetime import UTC, timedelta
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import TypeAdapter
from pydantic import ValidationError as PydanticValidationError

from retrievalloom.domain.base import (
    Digest,
    EngineId,
    FrozenModel,
    Identifier,
    VersionedModel,
    check_metadata_keys,
    utc_now,
)
from retrievalloom.domain.hashing import (
    canonical_json,
    digest_json,
    normalize_payload,
    normalize_text,
    random_id,
    sha256_digest,
    stable_id,
)

IDENTIFIER: TypeAdapter[str] = TypeAdapter(Identifier)
ENGINE_ID: TypeAdapter[str] = TypeAdapter(EngineId)
DIGEST: TypeAdapter[str] = TypeAdapter(Digest)
ENGINE_ID_PATTERN = re.compile(r"^[a-z]{1,8}_[0-9a-f]{32}$")

json_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text(max_size=8),
    lambda children: (
        st.lists(children, max_size=3) | st.dictionaries(st.text(max_size=4), children, max_size=3)
    ),
    max_leaves=10,
)


class _Frozen(FrozenModel):
    value: int


class _Versioned(VersionedModel):
    value: int = 0


# --- identifiers -----------------------------------------------------------------------


def test_identifier_is_nfc_normalised() -> None:
    decomposed = "cafe\u0301"
    assert IDENTIFIER.validate_python(decomposed) == "caf\u00e9"


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("", "length"),
        ("x" * 257, "length"),
        (" padded", "whitespace"),
        ("padded\t", "whitespace"),
        ("a\x00b", "control"),
        ("a\x85b", "control"),
    ],
)
def test_identifier_rejects_invalid(value: str, message: str) -> None:
    with pytest.raises(PydanticValidationError, match=message):
        IDENTIFIER.validate_python(value)


def test_identifier_accepts_boundaries() -> None:
    assert IDENTIFIER.validate_python("x" * 256) == "x" * 256
    assert IDENTIFIER.validate_python("a b/c:d") == "a b/c:d"


def test_engine_id_and_digest_validators() -> None:
    good = stable_id("doc", "x")
    assert ENGINE_ID.validate_python(good) == good
    for bad in ("DOC_" + "0" * 32, "doc_" + "0" * 31, "toolongpre_" + "0" * 32, "doc-" + "0" * 32):
        with pytest.raises(PydanticValidationError, match="invalid engine id"):
            ENGINE_ID.validate_python(bad)
    digest = sha256_digest("x")
    assert DIGEST.validate_python(digest) == digest
    for bad in ("sha256:" + "A" * 64, "md5:" + "0" * 64, "sha256:" + "0" * 63):
        with pytest.raises(PydanticValidationError, match="sha256"):
            DIGEST.validate_python(bad)


# --- base models -------------------------------------------------------------------------


def test_canonical_models_forbid_extra_nan_and_mutation() -> None:
    model = _Frozen(value=1)
    with pytest.raises(PydanticValidationError):
        model.value = 2  # type: ignore[misc]
    with pytest.raises(PydanticValidationError, match="Extra inputs"):
        _Frozen.model_validate({"value": 1, "other": 2})


def test_versioned_model_pins_schema_version() -> None:
    assert _Versioned().schema_version == "1.0.0"
    with pytest.raises(PydanticValidationError):
        _Versioned.model_validate({"schema_version": "2.0.0"})


def test_utc_now_is_aware_utc() -> None:
    now = utc_now()
    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)
    assert now.tzinfo == UTC


def test_check_metadata_keys() -> None:
    check_metadata_keys({"a": 1, "b_": 2})
    with pytest.raises(ValueError, match="non-empty"):
        check_metadata_keys({"": 1})
    with pytest.raises(ValueError, match="reserved"):
        check_metadata_keys({"_id": 1})


# --- hashing ---------------------------------------------------------------------------


def test_canonical_json_is_order_independent_and_compact() -> None:
    assert canonical_json({"b": 1, "a": [1, "é"]}) == '{"a":[1,"é"],"b":1}'
    assert canonical_json({"a": 1, "b": 2}) == canonical_json({"b": 2, "a": 1})
    with pytest.raises(ValueError, match="JSON compliant"):
        canonical_json(float("nan"))


def test_sha256_digest_str_and_bytes_agree() -> None:
    assert sha256_digest("héllo") == sha256_digest("héllo".encode())
    assert sha256_digest(b"") == (
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


@settings(max_examples=50)
@given(json_values)
def test_digest_json_is_deterministic_and_valid(value: Any) -> None:
    first = digest_json(value)
    assert first == digest_json(json.loads(json.dumps(value)))
    assert DIGEST.validate_python(first) == first


@settings(max_examples=50)
@given(st.from_regex(r"[a-z]{1,8}", fullmatch=True), st.lists(json_values, max_size=4))
def test_stable_id_is_deterministic_and_well_formed(prefix: str, parts: list[Any]) -> None:
    value = stable_id(prefix, *parts)
    assert value == stable_id(prefix, *parts)
    assert ENGINE_ID_PATTERN.match(value)
    assert value.startswith(prefix + "_")


def test_stable_id_length_prefix_prevents_concatenation_collisions() -> None:
    assert stable_id("doc", "ab", "c") != stable_id("doc", "a", "bc")
    assert stable_id("doc", "abc") != stable_id("doc", "ab", "c")
    # Non-string parts are canonical-JSON encoded, so int 1 and the string "1" coincide.
    assert stable_id("doc", "1") == stable_id("doc", 1)
    assert stable_id("doc", "x") != stable_id("obj", "x")
    assert stable_id("doc", {"a": 1, "b": 2}) == stable_id("doc", {"b": 2, "a": 1})


def test_random_id_is_unique_and_well_formed() -> None:
    ids = {random_id("evt") for _ in range(50)}
    assert len(ids) == 50
    assert all(ENGINE_ID_PATTERN.match(value) for value in ids)


def test_normalize_text_folds_newlines_and_nfc() -> None:
    assert normalize_text("a\r\nb\rc\nd") == "a\nb\nc\nd"
    assert normalize_text("cafe\u0301") == "caf\u00e9"
    with pytest.raises(ValueError, match="unsupported hash profile"):
        normalize_text("x", profile="rl-hash-v0")


def test_normalize_payload_recurses_into_containers() -> None:
    payload = {"t": "a\r\n", "l": ["b\r", ("c\r\n", 1)], "n": None, "i": 3}
    assert normalize_payload(payload) == {"t": "a\n", "l": ["b\n", ["c\n", 1]], "n": None, "i": 3}
    with pytest.raises(ValueError, match="unsupported hash profile"):
        normalize_payload(["x"], profile="nope")
