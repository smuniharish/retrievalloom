---
hide:
  - navigation
  - toc
---

<div class="rl-hero" markdown>

# RetrievalLoom

<p class="rl-tagline">Hybrid search for async Python applications. Find the
right passage whether the user types an exact code, a typo or a paraphrase, and
see exactly which document version it came from.</p>

[Get started](getting-started/index.md){ .md-button .md-button--primary }
[Quickstart](getting-started/quickstart.md){ .md-button }
[API reference](api/index.md){ .md-button }

</div>

## Why RetrievalLoom

A support assistant has to answer "What does error `E1042` mean?", "How do I
fix a conflcted copy?" and "How do I get my money back?" in the same hour. The
first needs exact matching, the second tolerates a typo, and the third shares no
words with the article that answers it. RetrievalLoom runs all of these searches
in one call and merges them into one ranked list.

It runs inside your application, so there is no service to deploy. You bring
the documents, and optionally your own models and stores. RetrievalLoom keeps
the indexes current as documents change, isolates your customers from each
other, and records where every result came from.

<div class="grid cards" markdown>

-   :material-source-merge:{ .lg .middle } **Every kind of match**

    ---

    Keyword, typo-tolerant, BM25 and semantic search run together. Graph and
    section-tree search can join the same ranked list.

-   :material-file-certificate-outline:{ .lg .middle } **Results you can cite**

    ---

    Each result names its document, version and the methods that found it, plus
    the span, pages and section path you supplied.

-   :material-sync:{ .lg .middle } **Always-current indexes**

    ---

    Edits and deletions reach every index. Searches never mix document versions
    or read a half-built index, even while a model change is rebuilt.

-   :material-account-group-outline:{ .lg .middle } **Built for many customers**

    ---

    Every call is scoped to a tenant and collection, with per-tenant limits and
    an access policy hook for your own rules.

-   :material-timer-sand:{ .lg .middle } **A responsive event loop**

    ---

    Model inference and index work run in worker threads with admission
    control, so your service keeps answering requests.

-   :material-puzzle-outline:{ .lg .middle } **Your own components**

    ---

    Add a custom embedding model, reranker, vector store or metadata store by
    implementing its public abstract base class. Contract kits verify the
    implementation.

</div>

    ## How it works

    Your application sends documents and queries. The engine stores every document
    version in the metadata store, keeps each channel's indexes in step with it,
    and answers a query by searching one consistent snapshot of every channel.

    ![RetrievalLoom accepts documents and queries from an application, keeps configured search channels aligned with metadata, and returns ranked evidence with source provenance](assets/diagrams/architecture-overview.png){ .diagram }

## Thirty-second tour

=== "pip"

    ```bash
    pip install retrievalloom
    ```

=== "uv"

    ```bash
    uv add retrievalloom
    ```

RetrievalLoom supports Python 3.12 to 3.14. Extras add knowledge-graph search
(`graph`), section-tree search (`structure`), PostgreSQL storage (`postgres`)
and the test kits for your own components (`testing`). See
[Installation](getting-started/installation.md).

The program below indexes three documents in memory and searches them. It needs
no model, server or network access:

```python
import asyncio

from retrievalloom import (
    DocumentInput,
    RetrievalEngine,
    Scope,
    SourceDescriptor,
    TextRepresentation,
)

TEXTS = {
    "asyncio": "asyncio runs coroutines on a single-threaded event loop.",
    "bm25": "BM25 ranks documents by term frequency and inverse document frequency.",
    "eiffel": "The Eiffel Tower in Paris was completed in 1889.",
}


async def main() -> None:
    scope = Scope(tenant_id="acme")
    documents = [
        DocumentInput(
            id=key,
            source=SourceDescriptor(uri=f"kb://{key}"),
            representations=[TextRepresentation(text=text)],
        )
        for key, text in TEXTS.items()
    ]
    async with RetrievalEngine() as engine:
        await engine.ingest(documents, scope=scope)
        result = await engine.retrieve("event loop coroutines", scope=scope, top_k=3)
        for item in result.evidence:
            source = item.sources[0]
            methods = sorted({hit.method.key for hit in item.provenance.hits})
            print(f"{source.document_id} v{source.document_version} {item.score:.4f} {methods}")


asyncio.run(main())
```

Output:

```text
asyncio v1 0.0164 ['index.fuzzy', 'index.sparse']
```

Each result names the document version it came from and the search methods
that found it. The [Quickstart](getting-started/quickstart.md) walks
through the same program step by step.

## Let your coding agent integrate it

The [RetrievalLoom Agent Skills](agent-skills/index.md) teach a coding agent the
workflow and rules for adding RetrievalLoom to an application, and for extending
it with your own models, stores and pipeline stages.

## Downloads

<p>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527retrievalloom%2527%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=downloads&amp;color=blue&amp;cacheSeconds=3600" alt="Total downloads"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520date%2520%253E%253D%2520toStartOfWeek%2528today%2528%2529%252C%25201%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=this%20week&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads this week"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520date%2520%253E%253D%2520toStartOfMonth%2528today%2528%2529%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=this%20month&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads this month"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520date%2520%253E%253D%2520toStartOfYear%2528today%2528%2529%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=this%20year&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads this year"></a>
</p>
<p>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528toString%2528y%2529%252C%2520%2527%253A%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520toYear%2528date%2529%2520y%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527retrievalloom%2527%2520GROUP%2520BY%2520y%2520ORDER%2520BY%2520y%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=by%20year&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by year"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253C%2520%2527B%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20A&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: A"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527B%2527%2520AND%2520country_code%2520%253C%2520%2527C%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20B&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: B"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527C%2527%2520AND%2520country_code%2520%253C%2520%2527D%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20C&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: C"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527D%2527%2520AND%2520country_code%2520%253C%2520%2527E%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20D&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: D"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527E%2527%2520AND%2520country_code%2520%253C%2520%2527F%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20E&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: E"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527F%2527%2520AND%2520country_code%2520%253C%2520%2527G%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20F&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: F"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527G%2527%2520AND%2520country_code%2520%253C%2520%2527H%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20G&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: G"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527H%2527%2520AND%2520country_code%2520%253C%2520%2527I%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20H&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: H"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527I%2527%2520AND%2520country_code%2520%253C%2520%2527J%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20I&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: I"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527J%2527%2520AND%2520country_code%2520%253C%2520%2527K%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20J&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: J"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527K%2527%2520AND%2520country_code%2520%253C%2520%2527L%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20K&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: K"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527L%2527%2520AND%2520country_code%2520%253C%2520%2527N%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20L-M&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: L-M"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527N%2527%2520AND%2520country_code%2520%253C%2520%2527Q%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20N-P&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: N-P"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527Q%2527%2520AND%2520country_code%2520%253C%2520%2527R%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20Q&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: Q"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527R%2527%2520AND%2520country_code%2520%253C%2520%2527S%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20R&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: R"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527S%2527%2520AND%2520country_code%2520%253C%2520%2527T%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20S&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: S"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527T%2527%2520AND%2520country_code%2520%253C%2520%2527U%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20T&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: T"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527U%2527%2520AND%2520country_code%2520%253C%2520%2527V%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20U&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: U"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527V%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20V-Z&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: V-Z"></a>
</p>

The badges use the [ClickHouse public PyPI dataset](https://clickpy.clickhouse.com/dashboard/retrievalloom).
The dataset refreshes daily, so counts may be about a day behind. Shields
caches badge images for up to an hour, and GitHub may cache them longer. Weeks
start on Monday in UTC. Country badges exclude missing country codes. Explore
interactive charts on the
[ClickPy dashboard](https://clickpy.clickhouse.com/dashboard/retrievalloom).

## Where to go next

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } __[Quickstart](getting-started/quickstart.md)__

    ---

    Ingest documents, retrieve evidence and read its provenance.

-   :material-book-open-variant:{ .lg .middle } __[User guide](guide/index.md)__

    ---

    Scopes, channels, generations and evidence, and how-to guides for common
    tasks.

-   :material-connection:{ .lg .middle } __[Integrations](integrations/index.md)__

    ---

    Qdrant, resourcera, refresh-engine, agxai, LadybugDB and TreeSearch.

-   :material-code-braces:{ .lg .middle } __[Examples](examples/index.md)__

    ---

    Runnable scenarios, from an in-memory quickstart to Qdrant with real
    models, with their expected output.

-   :material-speedometer:{ .lg .middle } __[Benchmarks](benchmarks.md)__

    ---

    Measured ingestion and retrieval latency, and how to reproduce it.

-   :material-api:{ .lg .middle } __[API reference](api/index.md)__

    ---

    Every public class and method, generated from the source.

</div>
