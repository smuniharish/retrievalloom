# Pre-chunked and multi-representation

Keep your own chunking and get exact page and section citations back. This example
ingests one document, a pump manual, with two representations:

- `body`, a `ChunksRepresentation` with the application's own chunks. Each chunk carries
  a character span into the original text, a page range and a section path. The engine
  indexes the chunks as given and reports these locations in every evidence source.
- `outline`, a `StructureRepresentation` with the manual's table of contents and a short
  abstract per section, used by the structure channel.

Each channel reads one representation per document: the index channel reads the first
`chunks` representation (or, if there is none, the first `text` representation), and the
structure channel reads the first `structure` representation. Every evidence source names
the representation it came from (`[body]` or `[outline]` in the output).

Matching chunks of the same section, or adjacent chunks, are merged into one evidence item
with one segment per chunk; the item's relations (`same_section`, `complementary`) say why.

## Requirements

The `structure` extra: `pip install "retrievalloom[structure]"`.

## Run

```bash
python examples/prechunked_and_multi_representation.py
```

## Source

<!-- fmt: off -->
```python title="examples/prechunked_and_multi_representation.py"
--8<-- "examples/prechunked_and_multi_representation.py"
```
<!-- fmt: on -->

## Expected output

```text
query: 'replace coolant filter'
  #1 score=0.0164 channels=['index'] relations=['complementary', 'same_section']
     [body] Manual > Maintenance (chars 43-86, p. 2): Replace the coolant filter every 500 hours.
     [body] Manual > Maintenance (chars 87-133, p. 2): Drain old coolant before fitting a new filter.
     [body] Manual > Introduction (chars 0-42, p. 1): The pump moves coolant through the engine.
  #2 score=0.0164 channels=['structure'] relations=[]
     [outline] Pump manual > Introduction: What the coolant pump does.
  #3 score=0.0161 channels=['structure'] relations=[]
     [outline] Pump manual > Maintenance: Service intervals for the filter.
query: 'warranty'
  #1 score=0.0164 channels=['structure'] relations=[]
     [outline] Pump manual > Warranty: Coverage period and exclusions.
  #2 score=0.0164 channels=['index'] relations=[]
     [body] Manual > Warranty (chars 134-185, p. 3): The warranty covers the pump housing for two years.
```
