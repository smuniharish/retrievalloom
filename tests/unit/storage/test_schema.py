"""``UTCDateTime`` column type."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest
from sqlalchemy.exc import StatementError

from retrievalloom.storage.schema import UTCDateTime

if TYPE_CHECKING:
    from retrievalloom.storage.sqlite import SQLiteMetadataStore


def test_bind_normalises_to_utc_and_round_trips() -> None:
    column = UTCDateTime()
    local = datetime(2026, 3, 4, 10, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    text = column.process_bind_param(local, None)
    assert text == "2026-03-04T04:30:00.000000+00:00"
    restored = column.process_result_value(text, None)
    assert restored == local
    assert restored is not None
    assert restored.tzinfo == UTC
    assert column.process_bind_param(None, None) is None
    assert column.process_result_value(None, None) is None


def test_naive_datetimes_are_rejected() -> None:
    with pytest.raises(ValueError, match="naive"):
        UTCDateTime().process_bind_param(datetime(2026, 1, 1), None)  # noqa: DTZ001


async def test_naive_datetime_never_reaches_the_database(store: SQLiteMetadataStore) -> None:
    naive = datetime(2026, 1, 1)  # noqa: DTZ001
    with pytest.raises(StatementError):
        async with store.transaction(write=True) as tx:
            await tx.put_reader_lease("g", "e", expires_at=naive, now=naive)
    async with store.transaction(write=False) as tx:
        assert await tx.count_reader_leases("g", now=datetime(2000, 1, 1, tzinfo=UTC)) == 0
