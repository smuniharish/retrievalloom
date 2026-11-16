"""Base model, primitive types and the injectable clock."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, JsonValue

SCHEMA_VERSION = "1.0.0"

_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_ENGINE_ID = re.compile(r"^[a-z]{1,8}_[0-9a-f]{32}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def _check_identifier(value: str) -> str:
    value = unicodedata.normalize("NFC", value)
    if not 1 <= len(value) <= 256:
        raise ValueError("identifier length must be between 1 and 256 characters")
    if value != value.strip():
        raise ValueError("identifier must not have leading or trailing whitespace")
    if _CONTROL.search(value):
        raise ValueError("identifier must not contain control characters")
    return value


def _check_engine_id(value: str) -> str:
    if not _ENGINE_ID.match(value):
        raise ValueError("invalid engine id")
    return value


def _check_digest(value: str) -> str:
    if not _DIGEST.match(value):
        raise ValueError("digest must have the form 'sha256:<64 hex>'")
    return value


Identifier = Annotated[str, AfterValidator(_check_identifier)]
EngineId = Annotated[str, AfterValidator(_check_engine_id)]
Digest = Annotated[str, AfterValidator(_check_digest)]
Metadata = dict[str, JsonValue]
NonNegativeInt = Annotated[int, Field(ge=0)]
PositiveInt = Annotated[int, Field(ge=1)]

Clock = Callable[[], datetime]


def utc_now() -> datetime:
    """Return the current time as an aware UTC datetime."""
    return datetime.now(UTC)


class CanonicalModel(BaseModel):
    """Base of every canonical model: strict and finite."""

    model_config = ConfigDict(
        extra="forbid",
        allow_inf_nan=False,
        str_strip_whitespace=False,
        validate_by_alias=True,
        validate_by_name=True,
    )


class FrozenModel(CanonicalModel):
    """Immutable canonical value object."""

    model_config = ConfigDict(frozen=True)


class VersionedModel(FrozenModel):
    """Top-level frozen artifact that carries the canonical schema version."""

    schema_version: Literal["1.0.0"] = SCHEMA_VERSION


def check_metadata_keys(metadata: dict[str, Any]) -> None:
    """Reject reserved (underscore-prefixed) metadata keys.

    Raises:
        ValueError: When a key is empty or reserved.
    """
    for key in metadata:
        if not key:
            raise ValueError("metadata keys must be non-empty strings")
        if key.startswith("_"):
            raise ValueError(f"metadata key {key!r} is reserved")
