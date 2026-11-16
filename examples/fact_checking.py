"""Claim verification: retrieve evidence for each claim and judge it with NLI.

A support-quality bot checks statements before they are sent to customers ("Does AcmeSync
support PAC files?"). For every claim the engine retrieves candidate passages, reranks them
with ``BgeCrossEncoderReranker`` and lets ``UltraBertNLI`` (``NLIABC``, local
``models/UltraBERT-150M-nli``) judge each passage against the claim
(``retrieve(..., claims=[...])``). Every candidate carries one judgement per claim
(``hypothesis_ref="claim:0"``); evidence items summarise them in ``nli_summary``, and
selection is contradiction-closed: an item contradicting a selected one is returned even
beyond ``top_k`` and linked through ``linked_evidence_ids``.

The example turns the judgements into a verdict per claim: *supported*, *refuted*,
*disputed* (sources disagree, e.g. an outdated forum post) or *not enough evidence*.

Requires the local models::

    $env:RL_EXAMPLES_MODELS_DIR="models"
    python examples/fact_checking.py
"""

import asyncio

from _shared import load, quiet_logs, require
from adapters import BgeCrossEncoderReranker, UltraBertNLI

from retrievalloom import (
    Components,
    DocumentInput,
    LifecycleConfig,
    NLIConfig,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    TextRepresentation,
    create_retrieval_engine,
)
from retrievalloom.domain.retrieval import NLILabel

SCOPE = Scope(tenant_id="acme", collection="fact-check")
CONFIDENT = 0.6
RELEVANT = 0.05


def verdict(supporting: list[str], contradicting: list[str]) -> str:
    if supporting and contradicting:
        return "disputed"
    if supporting:
        return "supported"
    if contradicting:
        return "refuted"
    return "not enough evidence"


async def main() -> None:
    quiet_logs()
    settings = require("MODELS_DIR")
    if settings is None:
        return
    models = settings["MODELS_DIR"]
    data = load("claims.json")
    engine = create_retrieval_engine(
        retrieval=RetrievalConfig(nli=NLIConfig(enabled=True, nli_k=4), request_timeout=300.0),
        components=Components(
            reranker=BgeCrossEncoderReranker(f"{models}/bge-reranker-v2-m3", normalize=True),
            nli=UltraBertNLI(f"{models}/UltraBERT-150M-nli"),
        ),
        lifecycle=LifecycleConfig(background=False),
    )
    async with engine:
        await engine.ingest(
            [
                DocumentInput(
                    _id=source["id"],
                    metadata={"kind": source["kind"]},
                    representations=(TextRepresentation(text=source["text"]),),
                )
                for source in data["sources"]
            ],
            scope=SCOPE,
        )
        config = RetrievalExecutionConfig(include_candidates=True)
        for claim in data["claims"]:
            result = await engine.retrieve(
                claim, scope=SCOPE, top_k=2, claims=[claim], config=config
            )
            supporting: list[str] = []
            contradicting: list[str] = []
            print(f"claim: {claim}")
            for candidate in result.candidates or ():
                # Only the nli_k best reranked candidates are judged.
                judgements = [j for j in candidate.nli if j.hypothesis_ref == "claim:0"]
                if not judgements:
                    continue
                judgement = judgements[0]
                document = candidate.sources[0].document_id
                relevance = candidate.scores.rerank or 0.0
                print(
                    f"    {document:<17} relevance={relevance:.3f} "
                    f"{judgement.label.value:<13} {judgement.confidence:.2f}"
                )
                # NLI also labels unrelated passages; only relevant, confident judgements count.
                if relevance < RELEVANT or judgement.confidence < CONFIDENT:
                    continue
                if judgement.label == NLILabel.ENTAILMENT:
                    supporting.append(document)
                elif judgement.label == NLILabel.CONTRADICTION:
                    contradicting.append(document)
            print(f"  verdict: {verdict(supporting, contradicting).upper()}")
            print(f"    supported by {sorted(supporting)}, contradicted by {sorted(contradicting)}")
            flagged = [
                item.sources[0].document_id
                for item in result.evidence
                if item.nli_summary.has_contradiction
            ]
            print(f"    evidence returned: {len(result.evidence)}, with contradiction: {flagged}")


if __name__ == "__main__":
    asyncio.run(main())
