"""Record factories for the storage unit tests."""

from __future__ import annotations

from typing import Any

from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import random_id, sha256_digest, stable_id
from retrievalloom.domain.inputs import Scope, SourceDescriptor, TransformationRef
from retrievalloom.domain.records import (
    Channel,
    CoverageRecord,
    CoverageState,
    DerivedObject,
    DocumentRecord,
    DocumentVersion,
    EventStatus,
    GenerationState,
    IndexGeneration,
    IngestionEvent,
    LifecycleTask,
    ObjectKind,
    RepresentationSnapshot,
    SourceReference,
    TaskKind,
    TaskState,
)

SCOPE = Scope(tenant_id="unit-a")
OTHER = Scope(tenant_id="unit-b")
TRANSFORM = TransformationRef(name="chunker", version="1")


def uid(scope: Scope, document_id: str) -> str:
    return stable_id("doc", scope.key, document_id)


def document(scope: Scope, document_id: str, version: int = 1, **values: Any) -> DocumentRecord:
    now = utc_now()
    return DocumentRecord.model_validate(
        {
            "_id": document_id,
            "_counter": version,
            "_version": version,
            "_content_hash": sha256_digest(f"{document_id}:{version}"),
            "_created_at": now,
            "_updated_at": now,
            "document_uid": uid(scope, document_id),
            "scope": scope,
            "hash_profile": "unit",
            **values,
        }
    )


def version(
    scope: Scope, document_id: str, number: int, *, source: bool = False
) -> DocumentVersion:
    return DocumentVersion(
        document_uid=uid(scope, document_id),
        version=number,
        content_hash=sha256_digest(f"{document_id}:{number}"),
        hash_profile="unit",
        representation_digests={"text": sha256_digest("t")},
        metadata={"n": number},
        source=SourceDescriptor(uri="file://x") if source else None,
        created_at=utc_now(),
        event_id=f"evt-{number}",
    )


def snapshot(
    scope: Scope, document_id: str, number: int, rep: str, *, transformed: bool = False
) -> RepresentationSnapshot:
    return RepresentationSnapshot(
        document_uid=uid(scope, document_id),
        version=number,
        representation_id=rep,
        kind="text",
        payload={"text": rep},
        transformation=TRANSFORM if transformed else None,
        hash_profile="unit",
    )


def event(
    scope: Scope,
    event_id: str,
    *,
    status: EventStatus = EventStatus.PENDING,
    fingerprint: str = "fp",
    **values: Any,
) -> IngestionEvent:
    return IngestionEvent(
        event_id=event_id,
        scope=scope,
        document_id="doc-1",
        document_uid=uid(scope, "doc-1"),
        received_at=values.pop("received_at", utc_now()),
        status=status,
        content_hash=sha256_digest("c"),
        appearance_fingerprint=sha256_digest(fingerprint),
        **values,
    )


def obj(
    scope: Scope,
    name: str,
    *,
    document_id: str | None = "doc-1",
    number: int | None = 1,
    kind: ObjectKind = ObjectKind.CHUNK,
    transformed: bool = False,
    **values: Any,
) -> DerivedObject:
    source = SourceReference(
        scope=scope,
        document_id=document_id or "agg",
        document_uid=uid(scope, document_id or "agg"),
        document_version=number or 1,
        representation_id="text",
    )
    return DerivedObject(
        object_id=stable_id("obj", name),
        scope=scope,
        kind=kind,
        document_uid=None if document_id is None else uid(scope, document_id),
        version=number,
        representation_id="text" if document_id else None,
        text=name,
        sources=(source,),
        transformation=TRANSFORM if transformed else None,
        content_hash=sha256_digest(name),
        **values,
    )


def coverage(
    scope: Scope,
    document_id: str,
    state: CoverageState = CoverageState.PENDING,
    *,
    method: str = "keyword",
    instance: str = "",
    **values: Any,
) -> CoverageRecord:
    return CoverageRecord(
        scope_key=scope.key,
        document_uid=uid(scope, document_id),
        channel=Channel.INDEX,
        method=method,
        backend_instance_id=instance,
        state=state,
        updated_at=utc_now(),
        **values,
    )


def generation(
    scope: Scope,
    state: GenerationState = GenerationState.BUILDING,
    *,
    instance: str | None = None,
) -> IndexGeneration:
    return IndexGeneration(
        generation_id=random_id("gen"),
        scope_key=scope.key,
        channel=Channel.INDEX,
        method="keyword",
        backend_id="unit",
        backend_instance_id=instance,
        dependency_fingerprint=sha256_digest("deps"),
        state=state,
        created_at=utc_now(),
    )


def task(scope: Scope, state: TaskState = TaskState.QUEUED, **values: Any) -> LifecycleTask:
    now = utc_now()
    return LifecycleTask(
        task_id=random_id("task"),
        scope_key=scope.key,
        kind=TaskKind.REBUILD,
        state=state,
        created_at=now,
        updated_at=now,
        **values,
    )
