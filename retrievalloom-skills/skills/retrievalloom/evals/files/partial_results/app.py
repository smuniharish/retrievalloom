"""A service that returns partial results after every restart: the input of eval 2."""

from __future__ import annotations

import asyncio
from pathlib import Path

from retrievalloom import EngineResources, Scope, create_retrieval_engine

DATA = Path("var/retrievalloom")


async def main() -> None:
    engine = create_retrieval_engine(resources=EngineResources(data_dir=DATA))
    async with engine:
        result = await engine.retrieve("password reset", scope=Scope(tenant_id="acme"))
        # Prints "partial" on the first request after each restart.
        print(result.status.value, [w.code.value for w in result.warnings])
        for item in result.evidence:
            print(item.sources[0].document_id, item.display_text)


if __name__ == "__main__":
    asyncio.run(main())
