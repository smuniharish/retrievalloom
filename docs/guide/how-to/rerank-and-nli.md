# Rerank and verify with NLI

Put the best passage first, and flag passages that contradict each other. A
**reranker** reads the query and each top passage together and reorders them,
which usually improves the first few results. A **natural language inference
(NLI)** model checks whether each passage supports or contradicts the query and
any claims you pass, so you can warn users about conflicting sources. Both run
after the search methods have been merged, and both are optional.

| Stage | Component | Enabled by default | Applies to |
| --- | --- | --- | --- |
| Rerank | Built-in FlashRank, or `Components(reranker=...)` | Yes (`RerankingConfig.enabled`), when the model files are present | Top `rerank_k` (50) candidates with text |
| NLI | `Components(nli=...)` | No (`NLIConfig.enabled`) | Top `nli_k` (10) candidates with text |

Reranking only reorders the fused list. Fusion (RRF by default) needs no model,
so retrieval works with or without a reranker; without one the final score is
the channel-fusion score.

A missing model never breaks a search: when a stage is enabled but no component
is available, the stage is skipped.
Configured reranking without a reranker is skipped silently; a request that
asks for it explicitly with `RetrievalExecutionConfig(reranking=True)` gets a
`reranker_unavailable` warning. NLI adds a `stage_skipped` warning, with
severity `info` when it was enabled by configuration and `warning` when the
request asked for it.

## Use the built-in FlashRank reranker

`FlashRankReranker` is the built-in reranker. It runs a FlashRank pairwise
cross-encoder on ONNX Runtime through the offloader (`WorkloadKind.RERANK`).
When `Components.reranker` is not set and `RerankingConfig.enabled` is true,
the engine wires it automatically from `RerankingConfig`:

| Field | Default | Meaning |
| --- | --- | --- |
| `model_dir` | `<data_dir>/models`, or `~/.cache/retrievalloom/models` without a `data_dir` | Directory holding `<model_name>/<model file>` |
| `model_name` | `ms-marco-TinyBERT-L-2-v2` | A FlashRank pairwise model. Listwise models raise `ConfigurationError`. |
| `allow_download` | `False` | Let FlashRank download a missing model from Hugging Face |

```python
from pathlib import Path

from retrievalloom import RerankingConfig, RetrievalConfig, create_retrieval_engine

engine = create_retrieval_engine(
    retrieval=RetrievalConfig(reranking=RerankingConfig(model_dir=Path("models/flashrank")))
)
```

The engine never downloads the model by default. If the model file is missing,
the default reranker logs `reranker_unavailable` once at startup and is dropped:
requests keep the fused order, with no per-request warning, and engine health
does not list it. Set `RerankingConfig(enabled=False)` to turn reranking off.

To override the default, pass any `RerankerABC` as `Components(reranker=...)`.
You can also construct `FlashRankReranker` yourself:

```python
from retrievalloom import Components
from retrievalloom.evidence.rerank import FlashRankReranker

components = Components(reranker=FlashRankReranker("models/flashrank"))
```

| Argument | Default | Meaning |
| --- | --- | --- |
| `cache_dir` | required | Directory holding `<model_name>/<model file>` |
| `model_name` | `ms-marco-TinyBERT-L-2-v2` | A FlashRank pairwise model |
| `max_length` | 512 | Token limit per (query, passage) pair |
| `batch_size` | 32 | Pairs per ONNX call |
| `allow_download` | `False` | Let FlashRank download a missing model from Hugging Face |

FlashRank's bundled models are CC-BY-SA licensed.

### Provision the model files

The model is never downloaded for you, so a deployment never reaches out to the
internet unexpectedly. Place the files before you deploy. Note that a reranker
you supply yourself is kept even when its model is missing:
`initialize` logs `reranker_unavailable`, `health()` reports it unhealthy, and
every request degrades with a `reranker_unavailable` warning.

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
  with `allow_download=True` and the same `cache_dir`, initialize an engine
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

## Use your own reranker

To use a different model or a hosted reranking API, subclass `RerankerABC`. `rerank(query, items)` must return one `RerankScore`
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

## Check claims with NLI

An NLI model judges each candidate text (premise) against the query and every
claim passed to `retrieve(..., claims=[...])` (hypotheses). Up to 32 claims are
allowed per request; raise `EngineLimits.max_claims` to allow more.

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
members' judgments. `has_contradiction` is true when a member takes part in a
`contradicts` relation: one candidate is judged entailment and another
contradiction for the same hypothesis. Consolidation keeps linked items in the
result even past `top_k` (see [Evidence](../concepts/evidence.md)).

## Choose what happens on failure

`RerankingConfig.on_error` and `NLIConfig.on_error` accept `"degrade"`
(default) or `"fail"`. With `degrade`, an error or a deadline in the stage adds
a warning and the request continues without that stage. A reranker that raises
`BackendUnavailableError` produces `reranker_unavailable`; other errors produce
`stage_failed_degraded`. With `fail`, the error propagates.

## Next steps

- Run real cross-encoder and NLI models from a local directory in the
  [fact checking example](../../examples/fact-checking.md).
- Read NLI judgments on each result in [Evidence and provenance](../concepts/evidence.md).
