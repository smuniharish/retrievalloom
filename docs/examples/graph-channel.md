# Graph channel

This example enables the graph channel, which stores graphs in an embedded LadybugDB
database. The application supplies entities and relationships for each document in a
`GraphRepresentation`; three documents describe Ada Lovelace, Charles Babbage, Luigi
Menabrea and the Analytical Engine. The engine merges entities and relationships asserted
by several documents into one graph per scope.

At query time the default graph retriever looks for entities whose name or alias appears
in the query (here *Ada Lovelace*) and expands their neighbourhood up to
`GraphChannelConfig.depth` hops (2 in this example). Entities and relationships both
become evidence; a relationship's text is `source relation_type target`. The example
disables the index channel (`RetrievalExecutionConfig(index=False)`) so that every item
comes from the graph, and prints for each one:

- the hop distance from the seed entity,
- the documents that asserted it,
- the traversal path, built from the hit's `path_refs` (object ids), named here with the
  texts of the returned objects.

## Requirements

The `graph` extra: `pip install "retrievalloom[graph]"`.

## Run

```bash
python examples/graph_channel.py
```

## Source

<!-- fmt: off -->
```python title="examples/graph_channel.py"
--8<-- "examples/graph_channel.py"
```
<!-- fmt: on -->

## Expected output

```text
status=complete evidence=7
#1 score=0.0164 hops=0 'Ada Lovelace' from ['notes']
#2 score=0.0161 hops=1 'Analytical Engine' from ['design', 'lecture', 'notes']
     path: Ada Lovelace -> Ada Lovelace wrote_about Analytical Engine -> Analytical Engine
#3 score=0.0159 hops=1 'Ada Lovelace wrote_about Analytical Engine' from ['notes']
     path: Ada Lovelace -> Ada Lovelace wrote_about Analytical Engine
#4 score=0.0156 hops=2 'Luigi Menabrea' from ['lecture']
     path: Ada Lovelace -> Ada Lovelace wrote_about Analytical Engine -> Analytical Engine -> Luigi Menabrea described Analytical Engine -> Luigi Menabrea
#5 score=0.0154 hops=2 'Charles Babbage designed Analytical Engine' from ['design']
     path: Ada Lovelace -> Ada Lovelace wrote_about Analytical Engine -> Analytical Engine -> Charles Babbage designed Analytical Engine
#6 score=0.0152 hops=2 'Charles Babbage' from ['design', 'lecture']
     path: Ada Lovelace -> Ada Lovelace wrote_about Analytical Engine -> Analytical Engine -> Charles Babbage designed Analytical Engine -> Charles Babbage
#7 score=0.0149 hops=2 'Luigi Menabrea described Analytical Engine' from ['lecture']
     path: Ada Lovelace -> Ada Lovelace wrote_about Analytical Engine -> Analytical Engine -> Luigi Menabrea described Analytical Engine
```
