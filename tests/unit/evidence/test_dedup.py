"""Identity and content-hash deduplication (P1: nothing dropped, never across scopes)."""

from __future__ import annotations

from retrievalloom.domain.hashing import stable_id
from retrievalloom.domain.retrieval import CandidateFlags, NLILabel
from retrievalloom.evidence.dedup import IdentityAndHashDeduplicator, merge_candidates, unique
from tests.unit.evidence.helpers import OTHER_SCOPE, candidate, hit, judgement, source


def test_unique_keeps_first_seen_order() -> None:
    assert unique([3, 1, 3, 2, 1]) == (3, 1, 2)
    assert unique(["a", "B", "b"], key=str.lower) == ("a", "B")
    first, second = source("x"), source("x")
    assert first is not second
    assert unique([first, second, source("y")]) == (first, source("y"))


def test_merge_without_others_returns_representative() -> None:
    a = candidate("a")
    assert merge_candidates(a, []) is a


async def test_identity_stage_merges_hits_and_judgements() -> None:
    first = candidate("a", nli=[judgement(NLILabel.ENTAILMENT)])
    second = first.model_copy(
        update={
            "candidate_id": stable_id("cand", "a-dense"),
            "hits": (hit("a", method="dense"),),
            "nli": (judgement(NLILabel.ENTAILMENT), judgement(NLILabel.NEUTRAL, "h2")),
        }
    )
    result = await IdentityAndHashDeduplicator().deduplicate([first, second])
    (kept,) = result.kept
    assert kept.candidate_id == first.candidate_id
    assert kept.merged_object_ids == ()
    assert kept.hits == (*first.hits, *second.hits)
    assert kept.sources == first.sources
    assert [j.label for j in kept.nli] == [NLILabel.ENTAILMENT, NLILabel.NEUTRAL]
    assert result.merged == {first.candidate_id: (second.candidate_id,)}


async def test_content_hash_stage_is_transitive_and_keeps_provenance() -> None:
    head = candidate("x", content="shared", text=None, flags=CandidateFlags(stale_version=True))
    y1 = candidate("y", content="shared", text="the text", document="d2")
    y2 = y1.model_copy(
        update={"candidate_id": stable_id("cand", "y2"), "hits": (hit("y", method="dense"),)}
    )
    z = candidate("z", content="shared", document="d3", flags=CandidateFlags(partial_coverage=True))
    other = candidate("other")
    result = await IdentityAndHashDeduplicator().deduplicate([head, y1, other, y2, z])
    assert [c.candidate_id for c in result.kept] == [head.candidate_id, other.candidate_id]
    kept = result.kept[0]
    assert kept.object_id == head.object_id
    assert kept.merged_object_ids == tuple(sorted([y1.object_id, z.object_id]))
    assert kept.sources == (*head.sources, *y1.sources, *z.sources)
    assert kept.hits == (*head.hits, *y1.hits, *y2.hits, *z.hits)
    assert kept.flags == CandidateFlags(stale_version=True, partial_coverage=True)
    assert kept.text == "the text"
    assert result.merged == {
        head.candidate_id: (y1.candidate_id, y2.candidate_id, z.candidate_id),
    }


async def test_content_hash_stage_can_be_disabled() -> None:
    a = candidate("a", content="same")
    b = candidate("b", content="same")
    enabled = IdentityAndHashDeduplicator()
    disabled = IdentityAndHashDeduplicator(content_hash=False)
    assert len((await enabled.deduplicate([a, b])).kept) == 1
    result = await disabled.deduplicate([a, b])
    assert result.kept == (a, b)
    assert result.merged == {}
    assert enabled.info.fingerprint.digest != disabled.info.fingerprint.digest


async def test_never_merges_across_scopes() -> None:
    mine = candidate("a", content="same")
    theirs = candidate("a", content="same", scope=OTHER_SCOPE)
    result = await IdentityAndHashDeduplicator().deduplicate([mine, theirs])
    assert result.kept == (mine, theirs)


async def test_repeated_candidate_is_not_reported_as_absorbed() -> None:
    a = candidate("a")
    result = await IdentityAndHashDeduplicator().deduplicate([a, a])
    assert result.kept == (a,)
    assert result.merged == {}
