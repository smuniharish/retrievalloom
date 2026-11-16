"""Final evidence validation: provenance integrity and scope isolation for any consolidator."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

from retrievalloom.errors import ProvenanceError, TenantIsolationError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pydantic import BaseModel

    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.retrieval import Candidate, Evidence


def _keys(values: Sequence[BaseModel]) -> set[str]:
    return {value.model_dump_json() for value in values}


class EvidenceValidator:
    """Rejects evidence that fabricates, drops or leaks provenance.

    Enforced for every result:

    - every member is a known candidate and belongs to exactly one evidence item;
    - one segment per member, carrying exactly that candidate's sources;
    - evidence and provenance sources equal the union of member sources;
    - provenance hits equal the union of member hits;
    - every source and hit belongs to the request scope;
    - relations connect members, links point to other evidence, scores are finite.
    """

    def validate(
        self, scope: Scope, evidence: Sequence[Evidence], candidates: Sequence[Candidate]
    ) -> None:
        """Validate ``evidence`` built from ``candidates`` for ``scope``.

        Raises:
            TenantIsolationError: Evidence, a source or a hit belongs to another scope.
            ProvenanceError: Provenance is missing, fabricated or inconsistent.
        """
        known = {candidate.candidate_id: candidate for candidate in candidates}
        owner: dict[str, str] = {}
        for item in evidence:
            self._check_scope(scope, item)
            details = {"evidence_id": item.evidence_id}
            if not math.isfinite(item.score):
                raise ProvenanceError("evidence score must be finite", details=details)
            members = list(item.member_candidate_ids)
            if [segment.candidate_id for segment in item.segments] != members:
                raise ProvenanceError("segments must match members one to one", details=details)
            for candidate_id in members:
                if candidate_id not in known:
                    raise ProvenanceError(
                        "evidence references an unknown candidate",
                        details={**details, "candidate_id": candidate_id},
                    )
                if candidate_id in owner:
                    raise ProvenanceError(
                        "candidate belongs to more than one evidence item",
                        details={**details, "candidate_id": candidate_id},
                    )
                owner[candidate_id] = item.evidence_id
            group = [known[candidate_id] for candidate_id in members]
            for segment, candidate in zip(item.segments, group, strict=True):
                if segment.object_id != candidate.object_id or _keys(segment.sources) != _keys(
                    candidate.sources
                ):
                    raise ProvenanceError(
                        "segment provenance differs from its candidate",
                        details={**details, "candidate_id": candidate.candidate_id},
                    )
            member_sources = _keys([s for c in group for s in c.sources])
            if _keys(item.sources) != member_sources or (
                _keys(item.provenance.sources) != member_sources
            ):
                raise ProvenanceError(
                    "evidence sources must equal the union of member sources", details=details
                )
            if _keys(item.provenance.hits) != _keys([h for c in group for h in c.hits]):
                raise ProvenanceError(
                    "provenance hits must equal the union of member hits", details=details
                )
            member_set = set(members)
            for relation in item.relations:
                if relation.a not in member_set or relation.b not in member_set:
                    raise ProvenanceError("relation endpoints must be members", details=details)
            if item.evidence_id in item.linked_evidence_ids:
                raise ProvenanceError("evidence cannot link to itself", details=details)

    @staticmethod
    def _check_scope(scope: Scope, item: Evidence) -> None:
        details = {"evidence_id": item.evidence_id}
        if item.scope != scope:
            raise TenantIsolationError("evidence belongs to another scope", details=details)
        if any(source.scope != scope for source in item.sources):
            raise TenantIsolationError("evidence source belongs to another scope", details=details)
        if any(hit.object_ref.scope_key != scope.key for hit in item.provenance.hits):
            raise TenantIsolationError("evidence hit belongs to another scope", details=details)
