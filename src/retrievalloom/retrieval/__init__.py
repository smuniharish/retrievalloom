"""Retrieval execution: planning, snapshots, candidates and the evidence pipeline."""

from retrievalloom.retrieval.candidates import CandidateBatch, CandidateBuilder, candidate_id
from retrievalloom.retrieval.pipeline import PipelineComponents, RetrievalPipeline, result_status
from retrievalloom.retrieval.planning import ExecutionPlan, PlannedMethod, resolve_plan
from retrievalloom.retrieval.snapshot import Snapshot, SnapshotReader

__all__ = [
    "CandidateBatch",
    "CandidateBuilder",
    "ExecutionPlan",
    "PipelineComponents",
    "PlannedMethod",
    "RetrievalPipeline",
    "Snapshot",
    "SnapshotReader",
    "candidate_id",
    "resolve_plan",
    "result_status",
]
