"""Long structured documents: a policy manual searched by section, with near-duplicate merging.

Policies, manuals and contracts are long and hierarchical; answers live in one section and
readers need the section path ("Expenses > Submitting receipts") to trust them. The
application passes the manual's outline as a ``structure`` representation; the structure
channel (TreeSearch) ranks sections by title and text, while the index channel searches
the same corpus lexically. Two application components shape the final evidence:

- ``NearDuplicateDeduplicator`` (``DeduplicatorABC``) merges candidates whose text is
  almost identical, e.g. an intranet FAQ that copied a policy paragraph. The merged
  evidence keeps both sources, so the answer cites the policy *and* the FAQ;
- ``DocumentDiversityConsolidator`` (``EvidenceConsolidatorABC``) caps how many items one
  document contributes, so a long manual cannot crowd out every other source.

``StemmingTokenizer`` makes "receipts"/"receipt" and "submitting"/"submit" match.

Requires the ``structure`` extra. Run from the repository root::

    python examples/policy_manual.py
"""

import asyncio
from typing import Any

from _shared import load, quiet_logs
from adapters import DocumentDiversityConsolidator, NearDuplicateDeduplicator, StemmingTokenizer

from retrievalloom import (
    ChannelsConfig,
    Components,
    DocumentInput,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
    Scope,
    SourceDescriptor,
    StructureChannelConfig,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
    create_retrieval_engine,
)

SCOPE = Scope(tenant_id="acme", collection="policies")
FAQ = (
    "Expense FAQ: Upload receipts to the expense tool within 14 days of returning. "
    "Claims submitted later than 60 days are rejected."
)
NEWSLETTER = (
    "Reminder from finance: the expense tool now accepts photos of receipts from your phone."
)


def node(raw: dict[str, Any]) -> StructureNodeInput:
    return StructureNodeInput(
        title=raw["title"],
        text=raw.get("text", ""),
        children=tuple(node(child) for child in raw.get("children", ())),
    )


def corpus() -> list[DocumentInput]:
    manual = load("travel_policy.json")
    return [
        DocumentInput(
            _id=manual["id"],
            source=SourceDescriptor(uri="intranet://policies/travel", title=manual["title"]),
            representations=(StructureRepresentation(root=node(manual)),),
        ),
        DocumentInput(
            _id="expense-faq",
            source=SourceDescriptor(uri="intranet://faq/expenses", title="Expense FAQ"),
            representations=(TextRepresentation(text=FAQ),),
        ),
        DocumentInput(
            _id="finance-news",
            source=SourceDescriptor(uri="intranet://news/finance", title="Finance newsletter"),
            representations=(TextRepresentation(text=NEWSLETTER),),
        ),
    ]


def location(item: Any) -> str:
    """Every cited place: document id plus section path (merged duplicates cite several)."""
    places = {
        f"{source.document_id}: {' > '.join(source.section_path) or '(whole text)'}"
        for source in item.sources
    }
    return " | ".join(sorted(places))


async def main() -> None:
    quiet_logs()
    engine = create_retrieval_engine(
        retrieval=RetrievalConfig(
            channels=ChannelsConfig(structure=StructureChannelConfig(enabled=True)),
            reranking=RerankingConfig(enabled=False),
        ),
        components=Components(
            tokenizer=StemmingTokenizer(),
            deduplicator=NearDuplicateDeduplicator(threshold=0.7),
            consolidator=DocumentDiversityConsolidator(max_per_document=2),
        ),
        lifecycle=LifecycleConfig(background=False),
    )
    async with engine:
        await engine.ingest(corpus(), scope=SCOPE)
        for question in (
            "When do I have to submit my receipt?",
            "hotel price limit per night in Europe",
            "Can I fly business class?",
            "security briefing for risky trips",
        ):
            result = await engine.retrieve(question, scope=SCOPE, top_k=3)
            print(f"Q: {question}")
            for rank, item in enumerate(result.evidence, start=1):
                channels = sorted({hit.method.channel.value for hit in item.provenance.hits})
                print(f"  #{rank} {channels} {location(item)}")
                print(f"      {(item.display_text or '')[:110]}")
            manual_items = sum(
                item.sources[0].document_id == "travel-policy" for item in result.evidence
            )
            print(f"  items from the manual: {manual_items} (cap 2)")


if __name__ == "__main__":
    asyncio.run(main())
