"""Customer-support knowledge base: hybrid retrieval with local models on Qdrant.

A help-desk assistant must find the right article for short, messy questions: product
names, typos, synonyms, German and English articles. This example wires the full index
channel with application-supplied components:

- keyword method: a JSON glossary (``GlossaryKeywordDictionary``) maps synonyms such as
  "money back" or "MFA" to canonical terms, per tenant;
- fuzzy method: RapidFuzz tolerates typos ("conflcted", "devises");
- sparse method: the built-in BM25 index;
- dense method: ``BgeM3Embedding`` (local ``models/bge-m3``) on a Qdrant server;
- the four method lists are fused with RRF, then ``BgeCrossEncoderReranker``
  (``models/bge-reranker-v2-m3``) reorders the top candidates;
- article metadata (product, language) is filtered at query time, and per-request
  ``RetrievalExecutionConfig`` switches methods or reranking on and off.

Requires the local models and a Qdrant server (``examples/compose.yaml``)::

    $env:RL_EXAMPLES_MODELS_DIR="models"; $env:RL_EXAMPLES_QDRANT_URL="http://localhost:6333"
    python examples/customer_support.py
"""

import asyncio

from _shared import (
    DATA,
    doc_ids,
    drop_qdrant_collections,
    load,
    methods,
    quiet_logs,
    require,
    run_prefix,
)
from adapters import BgeCrossEncoderReranker, BgeM3Embedding, GlossaryKeywordDictionary, TTLCache

from retrievalloom import (
    And,
    Components,
    DocumentInput,
    Eq,
    LifecycleConfig,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    SourceDescriptor,
    TextRepresentation,
    create_retrieval_engine,
)
from retrievalloom.index.vector import QdrantVectorStore

SCOPE = Scope(tenant_id="acme", collection="help-center")


def articles() -> list[DocumentInput]:
    return [
        DocumentInput(
            _id=article["id"],
            metadata={"product": article["product"], "language": article["language"]},
            source=SourceDescriptor(
                uri=f"https://help.acme.example/{article['id']}", title=article["title"]
            ),
            representations=(TextRepresentation(text=article["text"]),),
        )
        for article in load("support_kb.json")
    ]


async def main() -> None:
    quiet_logs()
    settings = require("MODELS_DIR", "QDRANT_URL")
    if settings is None:
        return
    models, url = settings["MODELS_DIR"], settings["QDRANT_URL"]
    prefix = run_prefix("support")
    cache = TTLCache(ttl=300)
    components = Components(
        embedding=BgeM3Embedding(f"{models}/bge-m3"),
        vector_store=QdrantVectorStore(url=url, collection_prefix=prefix),
        reranker=BgeCrossEncoderReranker(f"{models}/bge-reranker-v2-m3", normalize=True),
        keyword_dictionary=GlossaryKeywordDictionary(DATA / "glossary.json"),
        cache=cache,
    )
    engine = create_retrieval_engine(
        retrieval=RetrievalConfig(request_timeout=300.0, method_timeout=240.0),
        components=components,
        lifecycle=LifecycleConfig(background=False),
    )
    try:
        async with engine:
            report = await engine.ingest(articles(), scope=SCOPE)
            print(f"ingested: { {o.value: n for o, n in report.outcomes.items()} }")
            status = await engine.status(scope=SCOPE)
            print(f"coverage: {dict(sorted(status.coverage.items()))}")

            questions = [
                (
                    "how do I get my money back for a card payment",
                    Eq(field="product", value="acmepay"),
                ),
                ("conflcted copy after editing on two devises", None),
                ("I lost my phone and cannot pass MFA", None),
                ("Konfliktkopie löschen", Eq(field="language", value="de")),
                (
                    "webhook signature",
                    And(
                        clauses=(
                            Eq(field="product", value="acmepay"),
                            Eq(field="language", value="en"),
                        )
                    ),
                ),
            ]
            for question, filters in questions:
                result = await engine.retrieve(question, scope=SCOPE, filters=filters, top_k=3)
                print(f"\nQ: {question}")
                for rank, item in enumerate(result.evidence, start=1):
                    source = item.sources[0]
                    print(
                        f"  #{rank} {source.document_id} score={item.score:.3f} [{methods(item)}]"
                    )
                    print(f"      {source.title} <{source.source_uri}>")

            # Per-request overrides: compare each method alone, with reranking switched off.
            question = "files disappear from my laptop to save disk space"
            print(f"\nper-method comparison for: {question!r}")
            for name in ("keyword", "fuzzy", "sparse", "dense"):
                config = RetrievalExecutionConfig(
                    reranking=False,
                    keyword=name == "keyword",
                    fuzzy=name == "fuzzy",
                    sparse=name == "sparse",
                    dense=name == "dense",
                )
                result = await engine.retrieve(question, scope=SCOPE, top_k=2, config=config)
                print(f"  {name:<7} -> {doc_ids(result.evidence)}")
            config = RetrievalExecutionConfig(include_candidates=True)
            result = await engine.retrieve(question, scope=SCOPE, top_k=2, config=config)
            print(f"  hybrid+rerank -> {doc_ids(result.evidence)}")
            for candidate in sorted(
                result.candidates or (), key=lambda c: -(c.scores.rerank or 0.0)
            )[:3]:
                print(
                    f"     {candidate.sources[0].document_id}: "
                    f"fused={candidate.scores.channel_fusion or 0.0:.4f} "
                    f"rerank={candidate.scores.rerank or 0.0:.3f}"
                )
            # Identical requests against an unchanged index snapshot are served by the cache.
            repeat = await engine.retrieve(
                questions[0][0], scope=SCOPE, filters=questions[0][1], top_k=3
            )
            print(f"\nrepeated question -> {doc_ids(repeat.evidence)}")
            print(f"cache: hits={cache.hits} misses={cache.misses}")
    finally:
        await drop_qdrant_collections(url, prefix)


if __name__ == "__main__":
    asyncio.run(main())
