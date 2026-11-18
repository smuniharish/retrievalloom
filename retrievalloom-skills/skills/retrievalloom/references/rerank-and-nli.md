# Rerank and NLI

Use this page to put the best passage first and to check claims against the
evidence. Both stages run after fusion, on the top candidates, and both are
optional: retrieval works without them.

| Stage | Component | Default | Applies to |
| --- | --- | --- | --- |
| Rerank | Built-in FlashRank, or `Components(reranker=...)` | On, when the model files are present | Top `rerank_k` (50) candidates |
| NLI | `Components(nli=...)` | Off | Top `nli_k` (10) candidates |

## Provision the default reranker

The engine never downloads the FlashRank model unless you allow it. It looks
in `RerankingConfig.model_dir`: `<data_dir>/models`, or
`~/.cache/retrievalloom/models` without a `data_dir`.

```text
models/flashrank/
  ms-marco-TinyBERT-L-2-v2/
    flashrank-TinyBERT-L-2-v2.onnx
    config.json
    tokenizer.json
    tokenizer_config.json
    special_tokens_map.json
```

```python
from pathlib import Path

from retrievalloom import RerankingConfig, RetrievalConfig, create_retrieval_engine
from retrievalloom.evidence.rerank import FlashRankReranker

model_dir = Path("models/flashrank")
assert FlashRankReranker(model_dir).model_path.is_file()  # never downloads
engine = create_retrieval_engine(
    retrieval=RetrievalConfig(reranking=RerankingConfig(model_dir=model_dir))
)
```

To get the files, either run once with `allow_download=True` on a machine
with network access and copy the directory, or unzip
`https://huggingface.co/prithivida/flashrank/resolve/main/<model_name>.zip`
into `model_dir`. `python scripts/verify_setup.py --model-dir models/flashrank`
reports whether the file is present. The bundled models are CC-BY-SA
licensed.

## Know what happens without a model

- Default reranker, model missing: dropped at startup with one
  `reranker_unavailable` log event. Results keep the fused order with no
  warning.
- A request that asks for reranking with
  `RetrievalExecutionConfig(reranking=True)` and has none gets a
  `reranker_unavailable` warning and a `partial` result.
- A reranker you supply whose model is missing stays wired: `health()`
  reports it unhealthy and every request degrades with
  `reranker_unavailable`.
- `RerankingConfig(enabled=False)` turns reranking off.

## Use your own reranker or NLI model

```python
from retrievalloom import Components, NLIConfig, RetrievalConfig, create_retrieval_engine

engine = create_retrieval_engine(
    components=Components(reranker=my_reranker, nli=my_nli),  # your RerankerABC and NLIABC
    retrieval=RetrievalConfig(nli=NLIConfig(enabled=True)),
)
```

Writing these classes is covered by the `retrievalloom-extend` skill.

## Check claims

```python
result = await engine.retrieve(
    "is the cache enabled", scope=scope, claims=["the cache is on by default"]
)
for item in result.evidence:
    summary = item.nli_summary
    print(item.sources[0].document_id, summary.entailment, summary.contradiction)
    if summary.has_contradiction:
        ...  # sources disagree: show both sides
```

- Up to 32 claims per request (`EngineLimits.max_claims`; raise it for more). The query and each claim are hypotheses; each
  candidate text is a premise.
- `has_contradiction` is true when one candidate entails a hypothesis and
  another contradicts it. Linked contradicting evidence is kept even past
  `top_k`, with an `evidence_truncated_contradictions` info warning.
- NLI enabled without a model adds `stage_skipped` (`info` from
  configuration, `warning` when the request asked for it).

## Choose the failure policy

`RerankingConfig.on_error` and `NLIConfig.on_error` take `degrade` (default)
or `fail`. With `degrade`, an error adds a warning (`reranker_unavailable` or
`stage_failed_degraded`) and the request continues without the stage. With
`fail`, the error propagates.
