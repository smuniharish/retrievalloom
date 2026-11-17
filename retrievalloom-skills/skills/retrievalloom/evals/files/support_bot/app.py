"""A support bot with naive substring search: the starting point of eval 1."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class Article:
    id: str
    tenant: str
    title: str
    body: str
    url: str


ARTICLES = [
    Article(
        "e1042",
        "acme",
        "Error E1042",
        "E1042 means the license server rejected the token. Renew the token in Settings.",
        "https://kb.acme.test/e1042",
    ),
    Article(
        "vpn",
        "acme",
        "Connect to the VPN",
        "Install the client, sign in with single sign-on, then choose the nearest gateway.",
        "https://kb.acme.test/vpn",
    ),
    Article(
        "billing",
        "globex",
        "Change the billing address",
        "Billing administrators update the address under Account, then Invoices.",
        "https://kb.globex.test/billing",
    ),
]


async def search(tenant: str, question: str, top_k: int = 3) -> list[Article]:
    words = question.lower().split()
    hits = [a for a in ARTICLES if any(word in a.body.lower() for word in words)]
    return hits[:top_k]


async def answer(tenant: str, question: str) -> str:
    hits = await search(tenant, question)
    if not hits:
        return "Sorry, I could not find anything."
    return "\n".join(f"{a.title}: {a.body} ({a.url})" for a in hits)


if __name__ == "__main__":
    print(asyncio.run(answer("acme", "what does eror E1042 mean")))
