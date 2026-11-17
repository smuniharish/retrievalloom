# Knowledge graph

Platform notes (architecture pages, a team directory, an incident postmortem) are plain
text, but the questions people ask about them are about relationships: "who operates the
database behind Checkout, and where is it hosted?" The example builds a knowledge graph
from the notes at ingest and answers questions from the graph and the text together.

## How it works

- **`CapitalizedEntityExtractor`** (a custom `GraphExtractorABC`) turns capitalized phrases
  into typed entities and sentences such as "Team Harbor operates Postgres" into
  `A OPERATES B` relationships.
- **`AliasGraphNormalizer`** (a custom `GraphNormalizerABC`) case-folds names and resolves
  aliases (`PG` → `Postgres`), so the postmortem's "PG failed over" joins the same entity
  as the architecture page.
- **The graph store:**
    - **`Neo4jGraphStore`** (a custom `GraphStoreABC`, official async `neo4j` driver) when
      `RL_EXAMPLES_NEO4J_URL` is set. Nodes and edges carry the scope and graph generation,
      so tenants and generations stay isolated as they do in the built-in store.
    - The embedded **LadybugDB** store from the `graph` extra otherwise.
- **The default `NeighborhoodGraphRetriever`** expands the entities named in a question up
  to two hops and reports the path for each hit. Its contract only reads through
  `GraphStoreABC`, so no Neo4j-specific retriever is needed.
- **Channel fusion.** The graph channel and the index channel are fused with reciprocal
  rank fusion, so an answer cites facts and source passages.

The Neo4j run stores its nodes under a unique per-run label and drops that label at the end.

## Requirements

- Embedded: the `graph` extra.
- Neo4j: the `examples` dependency group and a Neo4j server from `examples/compose.yaml`.

## Run

```bash
python examples/knowledge_graph.py
RL_EXAMPLES_NEO4J_URL=bolt://localhost:7687 python examples/knowledge_graph.py
```

The default credentials are `neo4j`/`retrievalloom`. Override them with
`RL_EXAMPLES_NEO4J_USER` and `RL_EXAMPLES_NEO4J_PASSWORD`.

## Source

<!-- fmt: off -->
```python title="examples/knowledge_graph.py"
--8<-- "examples/knowledge_graph.py"
```
<!-- fmt: on -->

## Expected output

The first line reads `graph store: neo4j (bolt://...)` when Neo4j is used. The results
are the same with either store.

```text
graph store: ladybug (embedded)
graph coverage: {'ready': 4}
Q: Who operates Postgres and where is it hosted? (graph channel only)
  hops=0 'Postgres' <- ['checkout-architecture', 'incident-2041', 'team-directory']
  hops=1 'Orders DB' <- ['checkout-architecture']
     path: Postgres -> Orders DB RUNS_ON Postgres -> Orders DB
  hops=1 'Team Harbor OPERATES Postgres' <- ['team-directory']
     path: Postgres -> Team Harbor OPERATES Postgres
  ...
  hops=1 'Postgres IS_HOSTED_IN Frankfurt Region' <- ['checkout-architecture']
     path: Postgres -> Postgres IS_HOSTED_IN Frankfurt Region
  hops=1 'Postgres FAILED_OVER_TO Frankfurt Region' <- ['incident-2041']
     path: Postgres -> Postgres FAILED_OVER_TO Frankfurt Region
  hops=1 'Frankfurt Region' <- ['checkout-architecture', 'incident-2041']
     path: Postgres -> Postgres FAILED_OVER_TO Frankfurt Region -> Frankfurt Region
Q: Which team leads the database behind Checkout and what happened in the incident? (graph + index fused)
  #1 ['graph'] 'Checkout Service'
  #2 ['index'] 'Maria Okafor leads Team Falcon. Team Falcon is on call for Checkout. Jonas Berg leads Team'
  #3 ['graph'] 'Payments Gateway'
  #4 ['index'] 'PG failed over to Frankfurt Region during the incident. Checkout returned errors for eleve'
  #5 ['index'] 'The Checkout Service depends on Payments Gateway. Checkout Service stores orders in Orders'
  #6 ['graph'] 'Checkout Service DEPENDS_ON Payments Gateway'
```

`Postgres` is linked to all three notes, including the postmortem that only says "PG".
That link is the alias normalizer at work. The fused answer mixes graph facts with the
passages that support them.

Regex extraction is deliberately simple. In production an LLM- or NER-based
`GraphExtractorABC` takes its place, and the rest of the wiring stays the same.
