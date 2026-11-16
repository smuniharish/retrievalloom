"""Deterministic hashing and identifier derivation (hash profile ``rl-hash-v1``)."""

from __future__ import annotations

import hashlib
import json
import unicodedata
import uuid
from typing import Any

HASH_PROFILE = "rl-hash-v1"
SUPPORTED_HASH_PROFILES = frozenset({HASH_PROFILE})


def canonical_json(value: Any) -> str:
    """Serialise ``value`` canonically: sorted keys, no whitespace, no NaN."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def sha256_digest(data: str | bytes) -> str:
    """Return ``sha256:<hex>`` of ``data`` (strings are UTF-8 encoded)."""
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def digest_json(value: Any) -> str:
    """Return the digest of the canonical JSON form of ``value``."""
    return sha256_digest(canonical_json(value))


def _encode_parts(parts: tuple[Any, ...]) -> bytes:
    encoded = bytearray()
    for part in parts:
        text = part if isinstance(part, str) else canonical_json(part)
        raw = text.encode("utf-8")
        encoded += str(len(raw)).encode("ascii") + b":" + raw
    return bytes(encoded)


def stable_id(prefix: str, *parts: Any) -> str:
    """Derive a deterministic engine id from a length-prefixed tuple encoding.

    Args:
        prefix: Lower-case id prefix such as ``doc`` or ``obj``.
        *parts: Values identifying the object; non-strings are canonical-JSON encoded.

    Returns:
        ``<prefix>_<32 hex>``.
    """
    digest = hashlib.sha256(prefix.encode("ascii") + b"|" + _encode_parts(parts)).hexdigest()
    return f"{prefix}_{digest[:32]}"


def random_id(prefix: str) -> str:
    """Return ``<prefix>_<uuid4 hex>``."""
    return f"{prefix}_{uuid.uuid4().hex}"


def normalize_text(text: str, profile: str = HASH_PROFILE) -> str:
    """Normalise text for hashing under ``profile`` (NFC and newline folding).

    Raises:
        ValueError: When the hash profile is unknown.
    """
    if profile not in SUPPORTED_HASH_PROFILES:
        raise ValueError(f"unsupported hash profile {profile!r}")
    text = unicodedata.normalize("NFC", text)
    return text.replace("\r\n", "\n").replace("\r", "\n")


def normalize_payload(value: Any, profile: str = HASH_PROFILE) -> Any:
    """Recursively normalise every string inside a JSON-like payload."""
    if isinstance(value, str):
        return normalize_text(value, profile)
    if isinstance(value, dict):
        return {key: normalize_payload(item, profile) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [normalize_payload(item, profile) for item in value]
    return value
