# Customer support

A help center for two products, AcmeSync (file sync) and AcmePay (payments), with articles
in English and German. Support agents and a chat bot ask questions the way customers
write them: with typos ("conflcted copy"), with jargon ("MFA"), in German, or as a
paraphrase that shares no words with the article ("get my money back"). No single
retrieval method handles all of these, so the example runs them all and fuses the results.

## Wiring

| Need | Component |
| --- | --- |
| Exact product jargon and synonyms | keyword method + `GlossaryKeywordDictionary` (`data/glossary.json`: `MFA` → `two-factor authentication`) |
| Typos | fuzzy method (built in, RapidFuzz) |
| Term overlap | sparse method (built-in BM25) |
| Paraphrases and cross-language | dense method: `BgeM3Embedding` on `QdrantVectorStore` (Qdrant server) |
| Fusion | reciprocal rank fusion (the default `method_fusion`) |
| Precision at the top | `BgeCrossEncoderReranker` (bge-reranker-v2-m3, sigmoid-normalised) |
| Repeated questions | `TTLCache` caches whole retrieval results and query embeddings |

Articles carry `product` and `language` metadata, and queries narrow on them with
`Eq(...)` and `And(...)` filters. The per-method comparison uses a per-request
`RetrievalExecutionConfig` to switch single methods and the reranker on and off without
changing the engine. With `include_candidates=True` it prints the fused score next to the
reranker score, so you can see the cross-encoder reorder the candidates.

The Qdrant collections use a unique per-run prefix and are dropped at the end.

## Requirements

- Local models in `models/` (`bge-m3`, `bge-reranker-v2-m3`) and the `examples` dependency group.
- A Qdrant server: `RL_EXAMPLES_QDRANT_URL=http://localhost:6333`.

## Run

```bash
python examples/customer_support.py
```

## Source

<!-- fmt: off -->
```python title="examples/customer_support.py"
--8<-- "examples/customer_support.py"
```
<!-- fmt: on -->

## Expected output

Scores depend on the model build. The rankings below are what the tests assert.

```text
ingested: {'created': 10}
coverage: {'index.dense': {'ready': 10}, 'index.fuzzy': {'ready': 10}, 'index.keyword': {'ready': 10}, 'index.sparse': {'ready': 10}}

Q: how do I get my money back for a card payment
  #1 pay-refund score=0.463 [index.dense, index.fuzzy, index.keyword, index.sparse]
      Refunding a customer payment <https://help.acme.example/pay-refund>
  ...

Q: conflcted copy after editing on two devises
  #1 sync-conflicts score=0.779 [index.dense, index.fuzzy, index.sparse]
  ...

Q: I lost my phone and cannot pass MFA
  #1 sync-2fa score=0.015 [index.dense, index.fuzzy, index.keyword, index.sparse]
  ...

Q: Konfliktkopie löschen
  #1 sync-konflikte score=0.867 [index.dense, index.fuzzy, index.keyword, index.sparse]
  ...

Q: webhook signature
  #1 pay-webhooks score=0.983 [index.dense, index.fuzzy, index.sparse]
  ...

per-method comparison for: 'files disappear from my laptop to save disk space'
  keyword -> []
  fuzzy   -> ['sync-selective', 'sync-conflicts']
  sparse  -> ['sync-selective', 'sync-2fa']
  dense   -> ['sync-selective', 'sync-proxy']
  hybrid+rerank -> ['sync-selective', 'sync-proxy']
     sync-selective: fused=0.0164 rerank=0.034
     ...

repeated question -> ['pay-refund', 'pay-erstattung', 'pay-failed-card']
cache: hits=1 misses=10
```

Things to notice:

- The typo query is found by fuzzy and dense, not by keyword.
- `MFA` reaches the 2FA article through the glossary (`index.keyword`).
- The German article `pay-erstattung` ranks second for an English question through the
  dense method alone.
- The repeated question is a cache hit.
