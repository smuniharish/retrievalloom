"""Error hierarchy, config validators, logging helpers and the package facade."""

from __future__ import annotations

import inspect
import logging
from typing import Any

import pytest
import structlog
from pydantic import ValidationError as PydanticValidationError

import retrievalloom
from retrievalloom import errors
from retrievalloom._logging import ROOT_LOGGER_NAME, get_logger
from retrievalloom.config import (
    EngineConfig,
    IndexChannelConfig,
    IngestionConfig,
    MethodConfig,
    RetrievalConfig,
    TenantPolicy,
)
from retrievalloom.domain.records import Channel

ERROR_CLASSES = [
    cls
    for _, cls in inspect.getmembers(errors, inspect.isclass)
    if issubclass(cls, errors.RetrievalLoomError)
]


# --- errors ----------------------------------------------------------------------------


@pytest.mark.parametrize("cls", ERROR_CLASSES, ids=lambda cls: cls.__name__)
def test_every_error_uses_class_defaults(cls: type[errors.RetrievalLoomError]) -> None:
    exc = cls("safe message")
    assert isinstance(exc, errors.RetrievalLoomError)
    assert exc.code == cls.default_code
    assert exc.retryable is cls.default_retryable
    assert exc.details == {}
    assert str(exc) == "safe message"
    assert repr(exc) == f"{cls.__name__}(code={cls.default_code!r}, message='safe message')"


def test_error_codes_are_unique_and_exported() -> None:
    codes = [cls.default_code for cls in ERROR_CLASSES]
    assert len(codes) == len(set(codes))
    assert {cls.__name__ for cls in ERROR_CLASSES} == set(errors.__all__)


def test_error_overrides_and_details_are_copied() -> None:
    details: dict[str, Any] = {"backend": "qdrant"}
    exc = errors.BackendUnavailableError("down", code="custom", retryable=False, details=details)
    details["backend"] = "mutated"
    assert exc.code == "custom"
    assert exc.retryable is False
    assert exc.details == {"backend": "qdrant"}
    assert errors.ConfigurationError("x", retryable=True).retryable is True


def test_error_hierarchy_inherits_category() -> None:
    assert errors.CoverageError("x").category == "retrieval"
    assert isinstance(errors.CoverageError("x"), errors.RetrievalError)
    assert errors.EmbeddingValidationError("x").category == "validation"
    assert errors.VersionConflictError("x").retryable
    assert not errors.TenantIsolationError("x").retryable
    with pytest.raises(errors.BackendError):
        raise errors.BackendTimeoutError("slow")


# --- config ----------------------------------------------------------------------------


def test_ingestion_config_rejects_unknown_hash_profile() -> None:
    assert IngestionConfig().hash_profile == "rl-hash-v1"
    with pytest.raises(PydanticValidationError, match="unsupported hash profile"):
        IngestionConfig(hash_profile="rl-hash-v0")


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (IngestionConfig, {"concurrency": 0}),
        (IngestionConfig, {"concurrency": 257}),
        (IngestionConfig, {"index_wait_timeout": 0}),
        (IngestionConfig, {"index_wait": "some"}),
        (MethodConfig, {"weight": float("inf")}),
        (MethodConfig, {"weight": -0.1}),
        (MethodConfig, {"on_error": "ignore"}),
        (RetrievalConfig, {"request_timeout": 0}),
        (RetrievalConfig, {"tenant_policies": {" bad": {}}}),
        (EngineConfig, {"unknown": True}),
    ],
)
def test_config_rejects_invalid_values(model: Any, payload: dict[str, Any]) -> None:
    with pytest.raises(PydanticValidationError):
        model.model_validate(payload)


def test_index_channel_method_lookup() -> None:
    config = IndexChannelConfig.model_validate({"fuzzy": {"enabled": False, "score_cutoff": 70}})
    assert config.method("fuzzy") is config.fuzzy
    assert not config.method("fuzzy").enabled
    assert config.method("dense") is config.dense
    with pytest.raises(ValueError, match="'graph' is not a valid IndexMethod"):
        config.method("graph")


def test_tenant_policy_lookup_falls_back_to_default() -> None:
    special = TenantPolicy(max_top_k=5, allowed_channels=(Channel.INDEX,))
    config = RetrievalConfig(tenant_policies={"vip": special})
    assert config.tenant_policy("vip") is special
    assert config.tenant_policy("other") is config.default_tenant_policy


def test_engine_config_is_frozen_and_roundtrips() -> None:
    config = EngineConfig.model_validate({"retrieval": {"channels": {"graph": {"enabled": True}}}})
    assert config.retrieval.channels.graph.enabled
    assert not config.retrieval.channels.structure.enabled
    assert EngineConfig.model_validate_json(config.model_dump_json()) == config
    with pytest.raises(PydanticValidationError):
        config.log_content = True  # type: ignore[misc]


# --- logging ---------------------------------------------------------------------------


def test_library_root_logger_has_null_handler() -> None:
    handlers = logging.getLogger(ROOT_LOGGER_NAME).handlers
    assert any(isinstance(handler, logging.NullHandler) for handler in handlers)


@pytest.mark.parametrize(
    ("name", "expected"),
    [(None, "retrievalloom"), ("", "retrievalloom"), ("engine", "retrievalloom.engine")],
)
def test_get_logger_routes_to_named_stdlib_logger(
    name: str | None, expected: str, caplog: pytest.LogCaptureFixture
) -> None:
    structlog.configure(
        processors=[structlog.processors.KeyValueRenderer(key_order=["event"])],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )
    try:
        with caplog.at_level(logging.INFO, logger=ROOT_LOGGER_NAME):
            get_logger(name, engine_id="e1").info("hello", n=2)
    finally:
        structlog.reset_defaults()
    (record,) = [record for record in caplog.records if record.name == expected]
    message = record.getMessage()
    assert "event='hello'" in message
    assert "engine_id='e1'" in message
    assert "n=2" in message


# --- package facade --------------------------------------------------------------------


def test_package_exports_resolve() -> None:
    missing = [name for name in retrievalloom.__all__ if not hasattr(retrievalloom, name)]
    assert missing == []
    assert isinstance(retrievalloom.__version__, str)
    assert retrievalloom.__version__
