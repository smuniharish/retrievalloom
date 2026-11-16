# Own embeddings on Qdrant

This live example implements `EmbeddingModelABC` for
[bge-m3](https://huggingface.co/BAAI/bge-m3) with `transformers` and `torch`: 1024-dimension
dense vectors from CLS pooling, L2-normalised. The model loads in `initialize`, and every
blocking call (loading and inference) runs through the engine's offloader with
`WorkloadKind.EMBEDDING`, so it never blocks the event loop and is subject to the
engine's admission control. The component's `info` fingerprint names the model and its
dimensions.

Supplying an embedding model enables the `index.dense` method. `QdrantVectorStore` stores
the vectors on a Qdrant server; the keyword, fuzzy and sparse methods still run on their
in-process indexes. The two queries share no content words with the documents that
answer them, and dense retrieval finds those documents.

The example creates collections whose names start with `rl_examples` and deletes them
when it finishes.

## Requirements

- A Qdrant server, with its URL in `RL_EXAMPLES_QDRANT_URL` (fallback `RL_TEST_QDRANT_URL`).
  Start one with Podman:

    ```bash
    podman compose -f examples/compose.yaml up -d
    # or
    podman run -d --name rl-qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant
    ```

- The `bge-m3` model folder under `RL_EXAMPLES_MODELS_DIR` (fallback `RL_TEST_MODELS_DIR`,
  default `models`).
- `torch` and `transformers` (`uv sync --group examples` in a checkout).

If the Qdrant URL is not set or the model folder is missing, the example prints what is
missing and exits with status 0.

## Run

```bash
export RL_EXAMPLES_QDRANT_URL=http://localhost:6333
python examples/byo_embeddings_qdrant.py
```

## Source

<!-- fmt: off -->
```python title="examples/byo_embeddings_qdrant.py"
--8<-- "examples/byo_embeddings_qdrant.py"
```
<!-- fmt: on -->

## Expected output

Cosine similarities can differ slightly between machines.

```text
index.dense coverage: {'ready': 4}
'famous landmark in France' -> eiffel via ['index.dense', 'index.sparse']
   cosine=0.49 rank=1 text: The Eiffel Tower is a wrought-iron lattice tower in Paris, completed in 1889.
'how do green leaves make sugar' -> photosynthesis via ['index.dense']
   cosine=0.62 rank=1 text: Plants turn sunlight, water and carbon dioxide into glucose and oxygen.
```
