# Rerank and verify with NLI

Reranking and NLI run after channel fusion, on the fused candidate list. Both
are optional components.

| Stage | Component | Enabled by default | Applies to |
| --- | --- | --- | --- |
| Rerank | `Components(reranker=...)` | Yes (`RerankingConfig.enabled`) | Top `rerank_k` (50) candidates with text |
| NLI | `Components(nli=...)` | No (`NLIConfig.enabled`) | Top `nli_k` (10) candidates with text |

When a stage is enabled but no component is configured, the stage is skipped.
Reranking adds a `reranker_unavailable` warning and NLI a `stage_skipped`
warning. The severity is `info` when the stage was enabled by configuration and
`warning` when the request asked for it with `RetrievalExecutionConfig`.

## FlashRank

`FlashRankReranker` is the built-in reranker. It runs a FlashRank pairwise
cross-encoder on ONNX Runtime through the offloader (`WorkloadKind.RERANK`).
It is not wired by default: pass it explicitly.

```python
from retrievalloom import Components
from retrievalloom.evidence.rerank import FlashRankReranker

components = Components(reranker=FlashRankReranker("models/flashrank"))
```

| Argument | Default | Meaning |
| --- | --- | --- |
| `cache_dir` | required | Directory holding `<model_name>/<model file>` |
| `model_name` | `ms-marco-TinyBERT-L-2-v2` | A FlashRank pairwise model. Listwise models raise `ConfigurationError`. |
| `max_length` | 512 | Token limit per (query, passage) pair |
| `batch_size` | 32 | Pairs per ONNX call |
| `allow_download` | `False` | Let FlashRank download a missing model from Hugging Face |

FlashRank's bundled models are CC-BY-SA licensed.

### Provision the model files

By default the reranker never downloads. If the model file is missing,
`initialize` logs `reranker_unavailable`, `health()` reports the reranker
unhealthy, and every request degrades with a `reranker_unavailable` warning.

The expected file is `FlashRankReranker(...).model_path`:

```text
models/flashrank/
  ms-marco-TinyBERT-L-2-v2/
    flashrank-TinyBERT-L-2-v2.onnx
    config.json
    tokenizer.json
    tokenizer_config.json
    special_tokens_map.json
```

FlashRank reads all five files when it loads the model.

Two ways to provision it:

- **Download once.** On a machine with network access, construct the reranker
  with `allow_download=True` and the same `cache_dir`, initialise an engine
  once, then copy the directory to the target environment.
- **Manual.** Download
  `https://huggingface.co/prithivida/flashrank/resolve/main/<model_name>.zip`
  and unzip it into `cache_dir`. The archive contains the `<model_name>/`
  directory.

Check the result before you deploy:

```python
reranker = FlashRankReranker("models/flashrank")
assert reranker.model_path.is_file(), reranker.model_path
```

## Your own reranker

Subclass `RerankerABC`. `rerank(query, items)` must return one `RerankScore`
per `RerankItem`, matched by `item_id`, each score finite. Run the model
through the offloader:

```python
from collections.abc import Sequence

from retrievalloom.contracts.component import ComponentContext, ComponentInfo, make_info
from retrievalloom.contracts.models import RerankerABC, RerankItem, RerankScore
from retrievalloom.runtime.offloader import Offloader, WorkloadKind


class OverlapReranker(RerankerABC):
    """Toy reranker: counts shared words. Replace ``_score`` with your model."""

    def __init__(self) -> None:
        self._offloader: Offloader | None = None
        self._info = make_info(component="reranker", implementation="overlap", version="1")

    @property
    def info(self) -> ComponentInfo:
        return self._info

    async def initialize(self, context: ComponentContext) -> None:
        self._offloader = context.offloader

    def _score(self, query: str, items: list[RerankItem]) -> list[RerankScore]:
        words = set(query.lower().split())
        return [
            RerankScore(
                item_id=item.item_id, score=float(len(words & set(item.text.lower().split())))
            )
            for item in items
        ]

    async def rerank(self, query: str, items: Sequence[RerankItem]) -> Sequence[RerankScore]:
        assert self._offloader is not None
        return await self._offloader.run(WorkloadKind.RERANK, self._score, query, list(items))
```

After reranking, `Evidence.scores.rerank` holds the score of the best member.
Candidates beyond `rerank_k` keep their fusion order below the reranked ones.

## NLI

An NLI model judges each candidate text (premise) against the query and every
claim passed to `retrieve(..., claims=[...])` (hypotheses). Up to 32 claims are
allowed per request.

Subclass `NLIABC`. `judge(pairs)` returns one `NLIJudgement` per `NLIPair`, in
order, with the pair's `hypothesis_ref`:

```python
import asyncio
from collections.abc import Sequence

from retrievalloom import (
    Components,
    DocumentInput,
    EngineConfig,
    NLIConfig,
    RetrievalConfig,
    RetrievalEngine,
    Scope,
    TextRepresentation,
)
from retrievalloom.contracts.component import ComponentContext, ComponentInfo, make_info
from retrievalloom.contracts.models import NLIABC, NLIPair
from retrievalloom.domain.retrieval import NLIJudgement, NLILabel
from retrievalloom.runtime.offloader import Offloader, WorkloadKind


class NegationNLI(NLIABC):
    """Toy NLI: "not" in the premise contradicts. Replace ``_judge`` with your model."""

    def __init__(self) -> None:
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="nli",
            implementation="negation",
            version="1",
            model_identity="example/negation",
        )

    @property
    def info(self) -> ComponentInfo:
        return self._info

    async def initialize(self, context: ComponentContext) -> None:
        self._offloader = context.offloader

    def _judge(self, pairs: list[NLIPair]) -> list[NLIJudgement]:
        return [
            NLIJudgement(
                label=NLILabel.CONTRADICTION
                if " not " in f" {p.premise} "
                else NLILabel.ENTAILMENT,
                confidence=0.9,
                model_identity="example/negation",
                hypothesis_ref=p.hypothesis_ref,
            )
            for p in pairs
        ]

    async def judge(self, pairs: Sequence[NLIPair]) -> Sequence[NLIJudgement]:
        assert self._offloader is not None
        return await self._offloader.run(WorkloadKind.NLI, self._judge, list(pairs))


async def main() -> None:
    scope = Scope(tenant_id="acme")
    config = EngineConfig(retrieval=RetrievalConfig(nli=NLIConfig(enabled=True)))
    async with RetrievalEngine(config, Components(nli=NegationNLI())) as engine:
        await engine.ingest(
            [
                DocumentInput(
                    id="a",
                    representations=[TextRepresentation(text="the cache is enabled by default")],
                ),
                DocumentInput(
                    id="b",
                    representations=[
                        TextRepresentation(text="the cache is not enabled by default")
                    ],
                ),
            ],
            scope=scope,
        )
        result = await engine.retrieve(
            "is the cache enabled", scope=scope, claims=["the cache is on"]
        )
        for item in result.evidence:
            summary = item.nli_summary
            print(
                item.sources[0].document_id,
                summary.entailment,
                summary.contradiction,
                summary.has_contradiction,
            )


asyncio.run(main())
```

Both items report `has_contradiction` as true: `a` entails the hypotheses and `b`
contradicts them, so the two are linked.

Each evidence item carries an `nli_summary` with the label counts of its
members' judgements. `has_contradiction` is true when a member takes part in a
`contradicts` relation: one candidate is judged entailment and another
contradiction for the same hypothesis. Consolidation keeps linked items in the
result even past `top_k` (see [Evidence](../concepts/evidence.md)).

## Failure policy

`RerankingConfig.on_error` and `NLIConfig.on_error` accept `"degrade"`
(default) or `"fail"`. With `degrade`, an error or a deadline in the stage adds
a warning and the request continues without that stage. A reranker that raises
`BackendUnavailableError` produces `reranker_unavailable`; other errors produce
`stage_failed_degraded`. With `fail`, the error propagates.

The [custom reranker and NLI example](../../examples/custom-reranker-nli.md)
runs real cross-encoder and NLI models from a local directory.
