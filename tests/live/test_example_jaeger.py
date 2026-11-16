"""Live: the observability example's spans arrive in Jaeger with their names and attributes.

Runs ``examples/observability_jaeger.py`` against an OTLP/HTTP collector and queries the
Jaeger query API (v3, falling back to the v1 JSON API) for the printed trace id.

Environment: ``RL_TEST_JAEGER_URL`` (query API/UI, e.g. ``http://localhost:16686``) and
optionally ``RL_TEST_OTLP_URL`` (default: the Jaeger host on port 4318).
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
import pytest

pytestmark = [pytest.mark.live]

ROOT = Path(__file__).resolve().parents[2]
JAEGER = os.environ.get("RL_TEST_JAEGER_URL")
SERVICE = "retrievalloom-example"


def _otlp_url(jaeger: str) -> str:
    return os.environ.get("RL_TEST_OTLP_URL") or f"http://{urlsplit(jaeger).hostname}:4318"


def _value(value: dict[str, Any]) -> Any:
    for key in ("stringValue", "intValue", "boolValue", "doubleValue"):
        if key in value:
            return value[key]
    return value


def _v3_spans(client: httpx.Client, trace_id: str) -> dict[str, dict[str, Any]] | None:
    response = client.get(f"/api/v3/traces/{trace_id}")
    if response.status_code == 404:
        return None
    response.raise_for_status()
    spans: dict[str, dict[str, Any]] = {}
    for resource in response.json()["result"]["resourceSpans"]:
        service = {a["key"]: _value(a["value"]) for a in resource["resource"]["attributes"]}
        for scope in resource["scopeSpans"]:
            for span in scope["spans"]:
                attributes = {a["key"]: _value(a["value"]) for a in span.get("attributes", [])}
                attributes["service.name"] = service.get("service.name")
                spans[span["name"]] = attributes
    return spans


def _v1_spans(client: httpx.Client, trace_id: str) -> dict[str, dict[str, Any]] | None:
    response = client.get(f"/api/traces/{trace_id}")
    if response.status_code == 404:
        return None
    response.raise_for_status()
    spans: dict[str, dict[str, Any]] = {}
    for data in response.json()["data"]:
        for span in data["spans"]:
            attributes = {tag["key"]: tag["value"] for tag in span["tags"]}
            attributes["service.name"] = data["processes"][span["processID"]]["serviceName"]
            spans[span["operationName"]] = attributes
    return spans


def _services(client: httpx.Client) -> list[str]:
    for path, key in (("/api/v3/services", "services"), ("/api/services", "data")):
        response = client.get(path)
        if response.status_code == 200:
            return response.json()[key] or []
    raise AssertionError("Jaeger exposes neither /api/v3/services nor /api/services")


@pytest.mark.skipif(not JAEGER, reason="set RL_TEST_JAEGER_URL to run the Jaeger test")
def test_observability_example_spans_reach_jaeger() -> None:
    assert JAEGER is not None
    env = {
        **os.environ,
        "PYTHONIOENCODING": "utf-8",
        "RL_EXAMPLES_OTLP_URL": _otlp_url(JAEGER),
        "RL_EXAMPLES_JAEGER_URL": JAEGER,
    }
    completed = subprocess.run(  # noqa: S603
        [sys.executable, str(ROOT / "examples" / "observability_jaeger.py")],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    match = re.search(r"trace_id=([0-9a-f]{32}) request=(\S+)", completed.stdout)
    assert match, completed.stdout
    trace_id, request = match.groups()
    assert "exported 6 spans" in completed.stdout

    spans: dict[str, dict[str, Any]] | None = None
    with httpx.Client(base_url=JAEGER, timeout=10) as client:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            spans = _v3_spans(client, trace_id) or _v1_spans(client, trace_id)
            if spans and len(spans) >= 6:
                break
            time.sleep(1)
        assert SERVICE in _services(client)

    assert spans, f"trace {trace_id} never reached Jaeger"
    assert set(spans) >= {
        "support.request",
        "retrievalloom.ingest",
        "retrievalloom.retrieve",
        "execution.started",
        "retrieval.execution",
        "execution.completed",
    }
    assert all(span["service.name"] == SERVICE for span in spans.values())
    assert spans["support.request"]["app.request"] == request
    assert spans["support.request"]["rl.tenant"] == "acme"
    assert int(spans["retrievalloom.ingest"]["rl.documents"]) == 3
    retrieve = spans["retrievalloom.retrieve"]
    assert retrieve["rl.status"] == "complete"
    assert int(retrieve["rl.evidence"]) == 2
    assert str(retrieve["rl.request_id"]).startswith("req_")
    execution = spans["retrieval.execution"]
    assert execution["xai.tenant_id"] == "acme"
    assert execution["xai.application_id"] == "support-bot"
    assert retrieve["rl.request_id"] in execution["xai.payload"]
