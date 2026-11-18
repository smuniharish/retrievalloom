"""Check that RetrievalLoom is installed and works in this Python environment.

Runs offline, never downloads a model, and changes nothing outside memory. Prints
one PASS, FAIL, or INFO line per check:

    python scripts/verify_setup.py                       # core; extras reported
    python scripts/verify_setup.py --require postgres    # fail when an extra is missing
    python scripts/verify_setup.py --model-dir models    # where the FlashRank model lives
    python scripts/verify_setup.py --json                # one JSON object on stdout

Optional services are probed only when their environment variables are set:

    RETRIEVALLOOM_DSN            PostgreSQL DSN; connects and runs SELECT 1 (postgres extra)
    RETRIEVALLOOM_QDRANT_URL     Qdrant server URL; lists collections
    RETRIEVALLOOM_QDRANT_API_KEY optional Qdrant API key

Exit codes: 0 every check passed, 1 a check failed.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import platform
import re
import sys
from functools import partial
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

# Belt and braces: nothing below loads a model, and no library may fetch one.
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

type Range = tuple[tuple[int, ...], tuple[int, ...] | None]

REQUIRED: dict[str, Range] = {
    "agxai": ((1, 0, 2), (2,)),
    "ahocorasick-rs": ((1, 0, 3), (2,)),
    "aiosqlite": ((0, 22, 1), (1,)),
    "alembic": ((1, 20, 1), (2,)),
    "bm25s": ((0, 3, 16), (0, 4)),
    "cachetools": ((6,), (9,)),
    "flashrank": ((0, 2, 10), (0, 3)),
    "numpy": ((2, 2), None),
    "pydantic": ((2, 14, 3), (3,)),
    "qdrant-client": ((1, 20, 0), (2,)),
    "ranknexus": ((0, 1, 0), (0, 2)),
    "rapidfuzz": ((3, 14, 6), (4,)),
    "refresh-engine": ((1, 0, 0), (2,)),
    "resourcera": ((0, 2, 0), (0, 3)),
    "sqlalchemy": ((2, 1, 8), (2, 2)),
    "structlog": ((25, 5, 0), None),
}
# extra -> (distribution, range, module to import or None)
EXTRAS: dict[str, list[tuple[str, Range, str | None]]] = {
    "graph": [("ladybug", ((0, 22, 3), (0, 23)), "ladybug")],
    "structure": [("pytreesearch", ((1, 1, 1), (2,)), "treesearch")],
    "postgres": [("asyncpg", ((0, 30), None), "asyncpg")],
    "testing": [
        ("pytest", ((9, 2, 0), None), None),
        ("pytest-asyncio", ((1, 4, 1), None), None),
    ],
}

failures: list[str] = []
results: list[dict[str, str]] = []
as_json = False


def emit(status: str, message: str) -> None:
    results.append({"status": status, "message": message})
    if not as_json:
        print(f"{status}  {message}")


def report(passed: bool, message: str) -> bool:
    emit("PASS" if passed else "FAIL", message)
    if not passed:
        failures.append(message)
    return passed


def installed(distribution: str) -> str | None:
    try:
        return metadata.version(distribution)
    except metadata.PackageNotFoundError:
        return None


def release(version: str) -> tuple[int, ...]:
    """The numeric release of a version string: "1.4.1.post1" -> (1, 4, 1)."""
    numbers = []
    for part in version.split(".")[:3]:
        digits = re.match(r"\d+", part)
        numbers.append(int(digits.group()) if digits else 0)
    return tuple(numbers)


def describe(bounds: Range) -> str:
    lowest, below = bounds
    text = ">=" + ".".join(map(str, lowest))
    return text + ("," + "<" + ".".join(map(str, below)) if below else "")


def compatible(found: str | None, bounds: Range) -> bool:
    lowest, below = bounds
    if found is None:
        return False
    version = release(found)
    return lowest <= version and (below is None or version < below)


def check_environment(required_extras: set[str]) -> bool:
    report(
        (3, 12) <= sys.version_info[:2] < (3, 15),
        f"Python {platform.python_version()} (>=3.12,<3.15 required)",
    )
    version = installed("retrievalloom")
    if version is None:
        return report(False, "retrievalloom is not installed: pip install retrievalloom")
    report(release(version)[:2] == (0, 2), f"retrievalloom {version} (0.2.x expected)")
    for distribution, bounds in REQUIRED.items():
        found = installed(distribution)
        report(
            compatible(found, bounds),
            f"{distribution} {found or 'is not installed'} ({describe(bounds)} required)",
        )
    for extra, members in EXTRAS.items():
        check_extra(extra, members, required=extra in required_extras)
    return not failures


def check_extra(
    extra: str, members: list[tuple[str, Range, str | None]], *, required: bool
) -> None:
    hint = f'pip install "retrievalloom[{extra}]"'
    if all(installed(name) is None for name, _, _ in members):
        if required:
            report(False, f"extra '{extra}' is not installed: {hint}")
        else:
            emit("INFO", f"extra '{extra}' not installed ({hint} to enable it)")
        return
    for name, bounds, module in members:
        found = installed(name)
        ok = compatible(found, bounds)
        detail = ""
        if ok and module is not None:
            try:
                importlib.import_module(module)
            except Exception as error:  # a broken wheel, for example missing DLLs
                ok, detail = False, f"; import {module} failed: {type(error).__name__}: {error}"
        report(
            ok,
            f"extra '{extra}': {name} {found or 'is not installed'} "
            f"({describe(bounds)} required){detail}",
        )


def check_reranker_model(model_dir: Path | None) -> None:
    from retrievalloom.evidence.rerank import FlashRankReranker

    directory = model_dir or Path.home() / ".cache" / "retrievalloom" / "models"
    path = FlashRankReranker(directory).model_path
    if path.is_file():
        emit("INFO", f"FlashRank model found at {path}; the default reranker will load")
    else:
        emit(
            "INFO",
            f"FlashRank model not found at {path}; the default reranker is dropped at "
            "startup and results keep the fused order (see references/rerank-and-nli.md)",
        )


async def check_engine() -> None:
    from retrievalloom import (
        DocumentInput,
        Eq,
        LifecycleConfig,
        RerankingConfig,
        ResultStatus,
        RetrievalConfig,
        Scope,
        TextRepresentation,
        create_retrieval_engine,
    )

    acme, globex = Scope(tenant_id="acme"), Scope(tenant_id="globex")

    def document(doc_id: str, text: str, lang: str) -> DocumentInput:
        # The default keyword dictionary reads canonical terms from the "keywords" field.
        return DocumentInput(
            id=doc_id,
            metadata={"lang": lang, "keywords": ["asyncio"] if "asyncio" in text else []},
            representations=(TextRepresentation(text=text),),
        )

    engine = create_retrieval_engine(
        retrieval=RetrievalConfig(reranking=RerankingConfig(enabled=False)),
        lifecycle=LifecycleConfig(background=False),
    )
    async with engine:
        ingested = await engine.ingest(
            [
                document("loop", "The asyncio event loop runs coroutines.", "en"),
                document("bm25", "BM25 ranks documents by term frequency.", "en"),
                document("schleife", "Die asyncio Ereignisschleife plant Koroutinen.", "de"),
            ],
            scope=acme,
        )
        await engine.ingest([document("other", "asyncio event loop internals", "en")], scope=globex)
        report(
            not ingested.failed and not ingested.warnings,
            f"ingest: {sorted((k.value, v) for k, v in ingested.outcomes.items())}",
        )

        result = await engine.retrieve("asyncio event loop", scope=acme)
        found = [item.sources[0].document_id for item in result.evidence]
        methods = sorted(
            {hit.method.key for item in result.evidence for hit in item.provenance.hits}
        )
        report(
            result.status is ResultStatus.COMPLETE and found[:1] == ["loop"],
            f"retrieve: status={result.status.value}, evidence={found}, methods={methods}, "
            f"warnings={[warning.code.value for warning in result.warnings]}",
        )
        report("other" not in found, "scope isolation: another tenant's document is not visible")

        german = await engine.retrieve("asyncio", scope=acme, filters=Eq(field="lang", value="de"))
        german_ids = [item.sources[0].document_id for item in german.evidence]
        report(german_ids == ["schleife"], f"metadata filter lang=de: {german_ids}")

        deleted = await engine.delete(["loop"], scope=acme)
        after = await engine.retrieve("asyncio event loop", scope=acme)
        after_ids = [item.sources[0].document_id for item in after.evidence]
        report(
            deleted.deleted == ("loop",) and "loop" not in after_ids,
            f"delete hides the document: {after_ids}",
        )
        purged = await engine.purge(["loop", "missing"], scope=acme)
        status = await engine.status(scope=acme)
        report(
            purged.purged == ("loop",)
            and purged.not_found == ("missing",)
            and status.documents_active == 2,
            f"purge erases it: purged={list(purged.purged)}, active={status.documents_active}",
        )
        health = await engine.health()
        report(health.status.value == "healthy", f"engine health: {health.status.value}")


async def check_postgres(dsn: str) -> None:
    import asyncpg

    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        value = await connection.fetchval("SELECT 1")
        server = connection.get_server_version()
    finally:
        await connection.close()
    report(value == 1, f"PostgreSQL reachable (server {server.major}.{server.minor})")


async def check_qdrant(url: str) -> None:
    from qdrant_client import AsyncQdrantClient

    client = AsyncQdrantClient(url=url, api_key=os.environ.get("RETRIEVALLOOM_QDRANT_API_KEY"))
    try:
        collections = await client.get_collections()
    finally:
        await client.close()
    report(True, f"Qdrant reachable at {url} ({len(collections.collections)} collections)")


def main() -> int:
    global as_json
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--require",
        action="append",
        default=[],
        choices=sorted(EXTRAS),
        help="Fail when this extra is missing (repeatable).",
    )
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=None,
        help="RerankingConfig.model_dir to look in (default ~/.cache/retrievalloom/models).",
    )
    parser.add_argument("--json", action="store_true", help="Print one JSON object to stdout.")
    arguments = parser.parse_args()
    as_json = arguments.json
    if check_environment(set(arguments.require)):
        check_reranker_model(arguments.model_dir)
        checks: list[Callable[[], Coroutine[Any, Any, None]]] = [check_engine]
        dsn = os.environ.get("RETRIEVALLOOM_DSN")
        if dsn:
            checks.append(partial(check_postgres, dsn))
        else:
            emit("INFO", "PostgreSQL probe skipped (set RETRIEVALLOOM_DSN to run it)")
        url = os.environ.get("RETRIEVALLOOM_QDRANT_URL")
        if url:
            checks.append(partial(check_qdrant, url))
        else:
            emit("INFO", "Qdrant probe skipped (set RETRIEVALLOOM_QDRANT_URL to run it)")
        for check in checks:
            name = getattr(check, "func", check).__name__
            try:
                asyncio.run(check())
            except Exception as error:  # report every failing check, then the summary
                report(False, f"{name}: {type(error).__name__}: {error}")
    summary = "All checks passed." if not failures else f"{len(failures)} check(s) failed."
    if as_json:
        print(json.dumps({"ok": not failures, "summary": summary, "checks": results}, indent=2))
    else:
        print(summary)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
