"""Check an access policy through a real engine.

Copy this file next to your policy and change the import and the principals.
There is no contract kit for access policies, so these tests prove the two
promises directly: denied actions raise ``AccessDeniedError``, and the row
filter limits what a principal can retrieve, whatever filter the caller sends.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from access_policy_template import TeamAccessPolicy

from retrievalloom import (
    AccessDeniedError,
    Components,
    DocumentInput,
    Eq,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
    Scope,
    TextRepresentation,
    create_retrieval_engine,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from retrievalloom import RetrievalEngine, RetrievalResult

ACME = Scope(tenant_id="acme")
GLOBEX = Scope(tenant_id="globex")


def ids(result: RetrievalResult) -> list[str]:
    return [item.sources[0].document_id for item in result.evidence]


@pytest.fixture
async def engine() -> AsyncIterator[RetrievalEngine]:
    engine = create_retrieval_engine(
        components=Components(access_policy=TeamAccessPolicy()),
        retrieval=RetrievalConfig(reranking=RerankingConfig(enabled=False)),
        lifecycle=LifecycleConfig(background=False),
    )
    async with engine:
        await engine.ingest(
            [
                DocumentInput(
                    id=doc_id,
                    metadata={"team": team},
                    representations=(TextRepresentation(text=f"{team} setup guide"),),
                )
                for doc_id, team in (("vpn", "network"), ("pay", "finance"))
            ],
            scope=ACME,
            principal="alice",
        )
        yield engine


async def test_row_filter_limits_retrieval(engine: RetrievalEngine) -> None:
    assert sorted(ids(await engine.retrieve("setup guide", scope=ACME, principal="alice"))) == [
        "pay",
        "vpn",
    ]
    assert ids(await engine.retrieve("setup guide", scope=ACME, principal="bob")) == ["vpn"]


async def test_caller_filters_cannot_widen_the_row_filter(engine: RetrievalEngine) -> None:
    result = await engine.retrieve(
        "setup guide", scope=ACME, principal="bob", filters=Eq(field="team", value="finance")
    )
    assert ids(result) == []


async def test_read_only_principal_cannot_write(engine: RetrievalEngine) -> None:
    with pytest.raises(AccessDeniedError):
        await engine.delete(["vpn"], scope=ACME, principal="bob")
    with pytest.raises(AccessDeniedError):
        await engine.purge(["vpn"], scope=ACME, principal="bob")


async def test_other_tenants_and_anonymous_callers_are_denied(engine: RetrievalEngine) -> None:
    with pytest.raises(AccessDeniedError):
        await engine.retrieve("setup guide", scope=GLOBEX, principal="alice")
    with pytest.raises(AccessDeniedError):
        await engine.retrieve("setup guide", scope=ACME)
