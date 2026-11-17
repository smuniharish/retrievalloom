# Scopes and documents

Learn how to shape the data you send to RetrievalLoom. A **scope** keeps one
customer's documents apart from everyone else's. A **document** carries your
content, metadata and source, and RetrievalLoom versions it each time it
changes, so every search result can name the exact version it came from.

## Scope

Every engine call takes a `Scope`, which says whose data the call works on:

```python
from retrievalloom import Scope

scope = Scope(tenant_id="acme", namespace="support", collection="articles")
```

`namespace` and `collection` default to `"default"`. Everything RetrievalLoom
stores for a document, including its indexes and background work, belongs to
one scope, and a search never reads another scope. Per-tenant limits come from
the `TenantPolicy` of `scope.tenant_id`.

## Document

A `DocumentInput` is one document, such as an article, a manual page or a
ticket:

```python
from retrievalloom import DocumentInput, SourceDescriptor, TextRepresentation

document = DocumentInput(
    id="kb-42",
    metadata={"lang": "en", "product": "router", "keywords": ["WPA3"]},
    dataset="manuals",
    source=SourceDescriptor(uri="https://example.com/kb/42", title="Router setup"),
    representations=[TextRepresentation(text="...")],
)
```

| Field | Meaning |
| --- | --- |
| `id` | Your identifier, unique within the scope. The alias `_id` is accepted too. |
| `metadata` | JSON values used by filters and by the keyword dictionary. Keys must not start with `_`. |
| `dataset` | Optional grouping label, filterable as `_dataset`. |
| `source` | Where the document came from: `uri`, `media_type`, `title`, `external_version`. Copied into every evidence source. |
| `representations` | One or more views of the content, each with a unique `representation_id`. |
| `appearance_id`, `delivery` | Optional delivery information used for redelivery detection. |

Documents are validated strictly: unknown fields, `inf` and `NaN` are
rejected, and instances cannot be changed after creation.

## Representations

A representation is one view of a document's content: plain text, your own
chunks, a section tree or a knowledge graph. The `kind` field selects the
type.

| Representation | `kind` | Becomes | Indexed by |
| --- | --- | --- | --- |
| `TextRepresentation(text=...)` | `text` | One chunk | Index channel |
| `ChunksRepresentation(chunks=[ChunkInput(...)])` | `chunks` | One chunk per `ChunkInput` | Index channel |
| `StructureRepresentation(root=StructureNodeInput(...))` | `structure` | One node per tree node | Structure channel |
| `GraphRepresentation(entities=[...], relationships=[...])` | `graph` | Entity and relationship aggregates shared across documents | Graph channel |

RetrievalLoom does not parse files or split text, so you keep full control of
chunking. Produce chunks with the tool you already use and pass a
`ChunksRepresentation`. Each `ChunkInput` can carry a
`span`, `pages`, `section_path`, `parent_local_id`, `order` and its own
`metadata`. These locations are copied into evidence sources.

A document can carry several representations at once, for example chunks and
a section tree. Give each a distinct `representation_id`. Each channel indexes
exactly one of them, and this binding is fixed:

- Index channel: the first `chunks` representation, else the first `text`
  representation (indexed as one chunk).
- Structure channel: the first `structure` representation.
- Graph channel: the first `graph` representation. Without one, a configured
  `GraphExtractorABC` extracts a graph from the index channel's representation.

Further representations of the same kind are validated, stored with the
version and change the content hash, but they are not retrievable. The
[pre-chunked and multi-representation example](../../examples/prechunked-and-multi-representation.md)
shows this.

## Versions

You can send the same document as often as you like: RetrievalLoom stores a
new version only when its content changes. `ingest` hashes each document's
representations, its `dataset` and its metadata (except the keys listed in
`transient_metadata_keys`). The result per document is an `IngestOutcome`:

- `created`: first version.
- `updated`: the hash changed. A new version is stored and the old version's
  objects are superseded.
- `unchanged`: same hash. Nothing is re-derived. If only transient metadata or
  the `source` changed, the document's `metadata_revision` is bumped.
- `revived`: the document was deleted and is ingested again.
- `replayed`: this appearance was already committed. Nothing changes.
- `rejected` or `failed`: the document was not stored. The report says why.

Each ingest of a document is an *appearance* with an idempotency key. The key
comes from `appearance_id` if you set it, else from
`delivery.source_system` + `delivery.delivery_id`, else it is generated. Send
the same `appearance_id` again and you get `replayed`. When the key is
generated and the same content arrives again within
`redelivery_window_seconds`, the report carries a `possible_redelivery`
warning.

Evidence always names the `document_version` it came from. By default, a match
in a version that has since been replaced is dropped
(`allow_stale_versions=False`), so results never mix old and new text.

## Deletion

`delete(document_ids, scope=...)` marks documents deleted and removes their
objects from every index. The `DeleteReport` lists `deleted`,
`already_deleted` and `not_found` ids. The document stays in the metadata
store as a tombstone until `purge(document_ids, scope=...)` erases it, or until
maintenance erases it after `LifecycleConfig.purge_after`. See
[Deletion, erasure and retention](lifecycle.md#deletion-erasure-and-retention).

## Reserved filter fields

Besides your metadata, filters can use `_id` (the document id) and `_dataset`.
See [Filter and authorize](../how-to/filters-and-access.md).

## Next steps

- Choose which search methods run in [Channels and methods](channels.md).
- Index your own chunks with the
  [pre-chunked example](../../examples/prechunked-and-multi-representation.md).
