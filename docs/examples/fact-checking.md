# Fact checking

A support-quality bot checks statements before they reach customers. "AcmeSync supports PAC
files" may appear in an old forum post while the current article says the opposite.
Retrieval alone would return both passages. NLI tells the bot which passages support the
claim and which contradict it.

## How it works

- **Candidates.** `retrieve(query, claims=[claim])` retrieves candidates for the claim.
  `BgeCrossEncoderReranker` (normalized to 0..1) scores their relevance.
- **NLI.** `UltraBertNLI` (a custom `NLIABC` over `models/UltraBERT-150M-nli`) judges the
  `nli_k=4` best candidates against the claim. Each judgment has
  `hypothesis_ref="claim:0"`, a label (entailment, neutral or contradiction) and a
  confidence.
- **Contradiction-closed selection.** When one candidate entails the claim and another
  contradicts it, the engine records a contradiction between them. The evidence is
  contradiction-closed: an item that contradicts a selected item is returned even beyond
  `top_k`, and `nli_summary.has_contradiction` is set on both.
- **Verdicts.** The application turns the judgments into *supported*, *refuted*,
  *disputed* or *not enough evidence*. It counts only judgments that are confident
  (≥ 0.6) and on relevant passages (rerank ≥ 0.05).

The relevance threshold matters. NLI labels every pair it sees, including unrelated
passages. In the third claim, the refund article "contradicts" the trash-retention claim
with 0.94 confidence, although it has nothing to do with it. The engine's raw
`has_contradiction` flag therefore also marks `retention-policy` and `refund-article` as
contradicting, so the application filters by relevance before deciding.

## Requirements

Local models in `models/` (`bge-reranker-v2-m3`, `UltraBERT-150M-nli`) and the `examples`
dependency group.

## Run

```bash
python examples/fact_checking.py
```

## Source

<!-- fmt: off -->
```python title="examples/fact_checking.py"
--8<-- "examples/fact_checking.py"
```
<!-- fmt: on -->

## Expected output

```text
claim: AcmeSync supports PAC files for proxy configuration.
    forum-post        relevance=0.976 entailment    0.94
    proxy-article     relevance=0.938 contradiction 1.00
    release-notes     relevance=0.002 neutral       0.96
    retention-policy  relevance=0.000 neutral       0.94
  verdict: DISPUTED
    supported by ['forum-post'], contradicted by ['proxy-article']
    evidence returned: 2, with contradiction: ['forum-post', 'proxy-article']
claim: A card refund takes between five and ten business days.
    refund-article    relevance=0.995 entailment    0.98
    ...
  verdict: SUPPORTED
    supported by ['refund-article'], contradicted by []
    evidence returned: 2, with contradiction: []
claim: Deleted files stay in the trash for 30 days.
    retention-policy  relevance=0.977 entailment    0.98
    release-notes     relevance=0.280 neutral       0.59
    refund-article    relevance=0.000 contradiction 0.94
    forum-post        relevance=0.000 neutral       0.73
  verdict: SUPPORTED
    supported by ['retention-policy'], contradicted by []
    evidence returned: 3, with contradiction: ['retention-policy', 'refund-article']
claim: AcmePay had incidents in March.
    status-page       relevance=0.984 contradiction 1.00
    ...
  verdict: REFUTED
    supported by [], contradicted by ['status-page']
    evidence returned: 2, with contradiction: []
claim: AcmeSync has a native client for Linux.
    forum-post        relevance=0.128 neutral       0.98
    ...
  verdict: NOT ENOUGH EVIDENCE
    supported by [], contradicted by []
    evidence returned: 2, with contradiction: []
```

Things to notice:

- In the third claim the irrelevant contradicting passage is pulled into the evidence
  (3 items for `top_k=2`). That is contradiction closure at work.
- In the fourth claim nothing entails the claim, so no contradiction is recorded between
  passages. The refutation comes from the per-candidate judgments.
