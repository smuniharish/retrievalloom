"""Base component contract, identity and the per-engine component context."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import Field

from retrievalloom.domain.base import Clock, Digest, FrozenModel, Metadata
from retrievalloom.domain.hashing import digest_json
from retrievalloom.domain.reports import HealthReport, HealthStatus

if TYPE_CHECKING:
    from pathlib import Path

    from refresh_engine import MetricsSink

    from retrievalloom.runtime.offloader import Offloader


class ComponentFingerprint(FrozenModel):
    """Identity of a component configuration; a change makes dependent indexes stale."""

    component: str
    implementation: str
    implementation_version: str
    config_digest: Digest
    model_identity: str | None = None
    dimensions: int | None = None

    @property
    def digest(self) -> str:
        """Digest of the whole fingerprint."""
        return digest_json(self.model_dump(mode="json"))


class ComponentInfo(FrozenModel):
    """Name, implementation and fingerprint of a component."""

    name: str
    implementation: str
    version: str
    fingerprint: ComponentFingerprint
    details: Metadata = Field(default_factory=dict)


def make_info(
    *,
    component: str,
    implementation: str,
    version: str,
    config: Any = None,
    model_identity: str | None = None,
    dimensions: int | None = None,
    name: str | None = None,
) -> ComponentInfo:
    """Build a ``ComponentInfo`` whose fingerprint digests ``config``."""
    fingerprint = ComponentFingerprint(
        component=component,
        implementation=implementation,
        implementation_version=version,
        config_digest=digest_json(config if config is not None else {}),
        model_identity=model_identity,
        dimensions=dimensions,
    )
    return ComponentInfo(
        name=name or implementation,
        implementation=implementation,
        version=version,
        fingerprint=fingerprint,
    )


@dataclass(frozen=True, slots=True)
class ComponentContext:
    """Per-engine services handed to components at initialisation.

    Attributes:
        engine_id: Identifier of the owning engine instance.
        offloader: Runs blocking work off the event loop with admission control.
        clock: Injectable UTC clock.
        logger: structlog logger pre-bound with ``engine_id``.
        metrics: refresh-engine metrics sink.
        data_dir: Optional directory for durable component files.
        metadata_store: The engine's metadata store (read access for dictionaries).
    """

    engine_id: str
    offloader: Offloader
    clock: Clock
    logger: Any
    metrics: MetricsSink
    data_dir: Path | None = None
    metadata_store: Any = None


class ComponentABC(ABC):
    """Base of every pluggable component.

    ``initialize`` and ``close`` are idempotent. ``close`` never closes resources
    that the application passed in (they stay application-owned).
    """

    @property
    @abstractmethod
    def info(self) -> ComponentInfo:
        """Identity and fingerprint of the component."""

    async def initialize(self, context: ComponentContext) -> None:  # noqa: B027
        """Prepare the component; the default does nothing."""

    async def close(self) -> None:  # noqa: B027
        """Release engine-owned resources; the default does nothing."""

    async def health(self) -> HealthReport:
        """Report component health; the default reports healthy."""
        return HealthReport(component=self.info.name, status=HealthStatus.HEALTHY)
