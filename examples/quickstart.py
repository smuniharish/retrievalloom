"""Quickstart: ingest a few documents into an in-memory engine and retrieve evidence.

Everything runs in process with the default components: an in-memory SQLite metadata
store and the keyword, fuzzy and sparse (BM25) methods of the index channel. No
embedding model, reranker or external service is needed.

Run from the repository root::

    python examples/quickstart.py
"""

import asyncio

from _shared import methods, quiet_logs

from retrievalloom import (
    DocumentInput,
    Eq,
    LifecycleConfig,
    Scope,
    SourceDescriptor,
    TextRepresentation,
    create_retrieval_engine,
)

DOCUMENTS = {
    "asyncio": ("Python's asyncio library runs coroutines on a single-threaded event loop.", "en"),
    "bm25": ("BM25 ranks documents by term frequency and inverse document frequency.", "en"),
    "eiffel": ("The Eiffel Tower in Paris was completed in 1889 for the World's Fair.", "en"),
    "eventloop": ("Eine Ereignisschleife (event loop) verteilt Aufgaben an Koroutinen.", "de"),
}
# The default keyword dictionary reads canonical keywords from the "keywords" metadata field.
KEYWORDS = {"asyncio": ["asyncio"], "bm25": ["BM25"]}


async def main() -> None:
    quiet_logs()
    scope = Scope(tenant_id="acme")
    documents = [
        DocumentInput(
            _id=doc_id,
            metadata={"lang": lang, "keywords": KEYWORDS.get(doc_id, [])},
            source=SourceDescriptor(uri=f"kb://articles/{doc_id}", title=doc_id.title()),
            representations=(TextRepresentation(text=text),),
        )
        for doc_id, (text, lang) in DOCUMENTS.items()
    ]

    # background=False: no scheduler task; ingest indexes in the foreground (index_wait=all).
    async with create_retrieval_engine(lifecycle=LifecycleConfig(background=False)) as engine:
        report = await engine.ingest(documents, scope=scope)
        outcomes = {outcome.value: count for outcome, count in report.outcomes.items()}
        print(f"ingested: {outcomes}")

        result = await engine.retrieve("asyncio event loop with coroutines", scope=scope)
        print(f"status: {result.status.value}")
        print(f"warnings: {[warning.code.value for warning in result.warnings]}")
        for rank, item in enumerate(result.evidence, start=1):
            source = item.sources[0]
            print(f"#{rank} {source.document_id} v{source.document_version} score={item.score:.4f}")
            print(f"   text:    {item.display_text}")
            print(f"   source:  {source.source_uri} ({source.title})")
            print(f"   methods: {methods(item)}")
            matched = sorted(
                {
                    detail.term
                    for hit in item.provenance.hits
                    for detail in hit.matched
                    if detail.term
                }
            )
            print(f"   matched: {matched}")
            print(f"   stages:  {[stage.stage for stage in item.provenance.stage_trail]}")

        german = await engine.retrieve(
            "event loop", scope=scope, filters=Eq(field="lang", value="de")
        )
        print(f"lang=de filter: {[item.sources[0].document_id for item in german.evidence]}")


if __name__ == "__main__":
    asyncio.run(main())
