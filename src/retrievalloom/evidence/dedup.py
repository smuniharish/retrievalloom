"""Identity and exact content-hash deduplication (never across scopes, nothing dropped)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.evidence import DeduplicatorABC, DedupResult
from retrievalloom.domain.retrieval import CandidateFlags

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Iterable, Sequence

    from retrievalloom.domain.retrieval import Candidate


def unique[T](values: Iterable[T], key: Callable[[T], Hashable] | None = None) -> tuple[T, ...]:
    """Values in first-seen order without duplicates (by ``key`` or JSON value)."""
    seen: set[Hashable] = set()
    result: list[T] = []
    for value in values:
        marker = key(value) if key is not None else _value_key(value)
        if marker not in seen:
            seen.add(marker)
            result.append(value)
    return tuple(result)


def _value_key(value: Any) -> Hashable:
    if isinstance(value, BaseModel):
        return value.model_dump_json()
    return value


def merge_candidates(representative: Candidate, others: Sequence[Candidate]) -> Candidate:
    """Merge ``others`` into ``representative`` keeping every source, hit and judgement."""
    if not others:
        return representative
    group = (representative, *others)
    merged_ids = unique(
        object_id
        for candidate in group
        for object_id in (candidate.object_id, *candidate.merged_object_ids)
        if object_id != representative.object_id
    )
    return representative.model_copy(
        update={
            "merged_object_ids": tuple(sorted(merged_ids)),
            "sources": unique(source for candidate in group for source in candidate.sources),
            "hits": unique(hit for candidate in group for hit in candidate.hits),
            "nli": unique(judgement for candidate in group for judgement in candidate.nli),
            "flags": CandidateFlags(
                stale_version=any(candidate.flags.stale_version for candidate in group),
                partial_coverage=any(candidate.flags.partial_coverage for candidate in group),
            ),
            "text": representative.text
            if representative.text is not None
            else next((c.text for c in others if c.text is not None), None),
        }
    )


class IdentityAndHashDeduplicator(DeduplicatorABC):
    """Stage 1 merges candidates of the same object; stage 2 merges equal content hashes.

    Both stages work within one scope only. The first candidate of a group (input order is
    best-first) is the representative; the union of sources and hits is kept.

    Args:
        content_hash: Enable stage 2 (default ``True``).
    """

    def __init__(self, *, content_hash: bool = True) -> None:
        self._content_hash = content_hash
        self._info = make_info(
            component="deduplicator",
            implementation="identity_and_hash",
            version="1",
            config={"content_hash": content_hash},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def deduplicate(self, candidates: Sequence[Candidate]) -> DedupResult:
        """Merge duplicates, preserving the representative order."""
        stage_keys: list[Callable[[Candidate], Hashable]] = [
            lambda candidate: (candidate.scope.key, candidate.object_id)
        ]
        if self._content_hash:
            stage_keys.append(lambda candidate: (candidate.scope.key, candidate.content_hash))
        current = list(candidates)
        absorbed: dict[str, list[str]] = {}
        for stage_key in stage_keys:
            groups: dict[Hashable, list[Candidate]] = {}
            for candidate in current:
                groups.setdefault(stage_key(candidate), []).append(candidate)
            current = []
            for group in groups.values():
                head, rest = group[0], group[1:]
                current.append(merge_candidates(head, rest))
                for other in rest:
                    if other.candidate_id == head.candidate_id:
                        continue
                    absorbed.setdefault(head.candidate_id, []).extend(
                        [other.candidate_id, *absorbed.pop(other.candidate_id, [])]
                    )
        merged = {key: tuple(unique(values)) for key, values in absorbed.items()}
        return DedupResult(kept=tuple(current), merged=merged)
