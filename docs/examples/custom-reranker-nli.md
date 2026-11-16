# Custom reranker and NLI

This live example implements two model contracts with `transformers` and `torch`:

- `RerankerABC` with the cross-encoder
  [bge-reranker-v2-m3](https://huggingface.co/BAAI/bge-reranker-v2-m3);
- `NLIABC` with UltraBERT-150M-nli, whose weights ship their own modeling code
  (`trust_remote_code=True`).

Both load in `initialize` and run all blocking work through the engine's offloader with
the matching workload kind (`WorkloadKind.RERANK`, `WorkloadKind.NLI`). No vector store or
embedding model is used; the candidates come from the in-process lexical methods.

The query is *When was the Eiffel Tower completed?* and the application passes one claim,
*The Eiffel Tower was completed in 1889.* With `include_candidates=True` the example
prints:

- the candidate order after channel fusion and after reranking. Lexical fusion ranks a
  blog post that repeats the query's words first; the cross-encoder ranks the document
  that answers the question first;
- each candidate's NLI judgement against the query (`query`) and the claim (`claim:0`).

When one candidate entails a claim and another contradicts it, consolidation links the two
evidence items (relation `contradicts`, listed in `linked_evidence_ids`) and sets
`nli_summary.has_contradiction`. Selection is contradiction-closed: with `top_k=1` the
result still contains the contradicting item, because contradicting items are added beyond
`top_k` up to `ConsolidationConfig.contradiction_overflow` (by default half of `top_k`,
rounded up). Links to items that did not fit are reported as "not selected".

## Requirements

- The `bge-reranker-v2-m3` and `UltraBERT-150M-nli` model folders under
  `RL_EXAMPLES_MODELS_DIR` (fallback `RL_TEST_MODELS_DIR`, default `models`).
- `torch` and `transformers` (`uv sync --group examples` in a checkout).

No Qdrant server is needed. If a model folder is missing, the example prints where it
looked and exits with status 0.

## Run

```bash
python examples/custom_reranker_nli.py
```

## Source

<!-- fmt: off -->
```python title="examples/custom_reranker_nli.py"
--8<-- "examples/custom_reranker_nli.py"
```
<!-- fmt: on -->

## Expected output

Model scores can differ slightly between machines.

```text
fused order:    ['rumour', 'pyramid', 'height', 'history']
reranked order: ['history', 'rumour', 'pyramid', 'height']
  history: rerank=6.86 nli={'query': 'entailment', 'claim:0': 'entailment'}
  rumour: rerank=6.57 nli={'query': 'entailment', 'claim:0': 'contradiction'}
  pyramid: rerank=-3.29 nli={'query': 'neutral', 'claim:0': 'contradiction'}
  height: rerank=-5.88 nli={'query': 'neutral', 'claim:0': 'neutral'}
top_k=1 returned 2 evidence item(s):
  history: has_contradiction=True contradicts=['rumour'] (+1 not selected)
  rumour: has_contradiction=True contradicts=['history'] (+0 not selected)
```
