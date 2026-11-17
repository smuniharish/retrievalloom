# Models and text components

Plug in the embedding model, cross-encoder, NLI model, tokenizer, or graph
extractor the project already uses. Each one is a small base class in
`retrievalloom.contracts.models`.

## Embedding model

Passing an embedding model as `Components(embedding=...)` adds the dense
method.

| Member | What it must do |
| --- | --- |
| `info` | Built with `make_info(..., model_identity=..., dimensions=...)`. The kit checks both. |
| `dimensions` | The vector length, at least 1. |
| `max_batch_size` | Optional. The most texts per `embed_documents` call. Default 64. |
| `embed_documents(texts)` | One vector per text, in order. Batching must not change the vectors beyond `tolerance`. |
| `embed_query(text)` | One vector, repeatable for the same text. |

The engine rejects a result with the wrong number of vectors, the wrong
dimensions, non-finite values, or a zero norm. Changing `model_identity` or
`dimensions` changes the fingerprint, so the engine rebuilds the dense
generations.

Run local models through the offloader with `WorkloadKind.EMBEDDING`. Await
an async HTTP client directly. `assets/embedding_template.py` shows both
rules.

## Reranker

`rerank(query, items)` receives `RerankItem(item_id, text)` objects and
returns one `RerankScore(item_id, score)` per item. Higher scores rank
first.

- Score every item exactly once, with a finite score. Otherwise the engine
  treats the call as failed. Return `[]` for no items; the kit checks it.
- Raise `BackendUnavailableError` when the model cannot run. The engine then
  keeps the fused order and adds a `reranker_unavailable` warning. Any other
  error adds `stage_failed_degraded`. With `RerankingConfig(on_error="fail")`,
  the error reaches the caller instead.
- Run the model through the offloader with `WorkloadKind.RERANK`.

Pass it as `Components(reranker=...)`; it replaces FlashRank.
`assets/reranker_template.py` is a complete example.

## NLI model

`judge(pairs)` receives `NLIPair(pair_id, premise, hypothesis,
hypothesis_ref)` objects and returns one `NLIJudgement` per pair, in the same
order.

| `NLIJudgement` field | Value |
| --- | --- |
| `label` | `NLILabel.ENTAILMENT`, `CONTRADICTION`, or `NEUTRAL`. |
| `confidence` | Between 0 and 1. |
| `scores` | Optional score per label. |
| `model_identity` | Non-empty. |
| `hypothesis_ref` | Copied from the pair. |

A misaligned or short answer fails the NLI stage, which then degrades with
`stage_failed_degraded`, or raises with `NLIConfig(on_error="fail")`. Run the
model through the offloader with `WorkloadKind.NLI` and test it with
`NLIContract`. The reranker template has the same shape: change the method
and the return type.

## Tokenizer

`tokenize(texts)` returns one token list per text. The engine uses it for the
sparse and fuzzy methods, both when indexing (`IndexItem.tokens`) and for
queries. A wrong number of lists raises `ComponentContractError`. Put
settings such as stopwords or stemming into the fingerprint, because they
change the index. There is no kit: compare with `SimpleTokenizer`
(`retrievalloom.index.text`) in your own tests.

## Keyword dictionary

`entries(scope)` returns the `KeywordEntry(canonical, aliases)` list for a
scope. The keyword method matches only these surface forms.

`revision(scope)` returns a token that changes whenever the entries change.
The engine checks it on every indexing pass and rebuilds keyword generations
when it changes. The default digests `entries(scope)`. Override it with
something cheaper, such as a version column, when computing the entries reads
the whole scope.

The built-in `StaticKeywordDictionary(entries)` and
`MetadataKeywordDictionary(fields=("keywords",), static_entries=())` are in
`retrievalloom.index.text`.

## Graph extractor and normalizer

`GraphExtractorABC.extract(source)` receives an `ExtractionSource` with
`scope`, `document_id`, `document_uid`, `version`, `representation_id`,
`texts`, and `metadata`. It returns a `GraphExtraction` of
`GraphEntityInput` (`name`, optional `key`, `entity_type`, `aliases`, and
more) and `GraphRelationshipInput` (`source_key`, `target_key`,
`relation_type`, and more).

`GraphExtractorContract` checks two things: every relationship endpoint is
an extracted entity's `key` or `name`, and the same source gives the same
graph. Make an LLM extractor deterministic, for example with temperature 0
and a cache keyed by the source.

`GraphNormalizerABC.normalize(extraction, scope)` returns a `NormalizedGraph`
with canonical entity keys. It has no kit; `IdentityGraphNormalizer`
(`retrievalloom.graph.normalizer`) is the reference.
