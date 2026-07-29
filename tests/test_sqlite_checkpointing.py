from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable

import pytest

from jobstreaming import (
    AdapterRegistry,
    CheckpointCompatibilityError,
    CheckpointConflictError,
    CheckpointError,
    CheckpointWrite,
    JobEvent,
    JobPost,
    JobResponse,
    Scraper,
    SearchCheckpoint,
    SearchRequest,
    Site,
    SqliteCheckpointStore,
    stream_search,
)
from tools.benchmark_checkpoints import run_scenario


def _job(number: int) -> JobPost:
    return JobPost(
        id=f"job-{number}",
        title=f"Job {number}",
        job_url=f"https://example.test/jobs/{number}",
    )


class _RestartableAdapter(Scraper):
    def __init__(self, **_: object) -> None:
        super().__init__(Site.INDEED)

    def scrape(self, request, context=None) -> JobResponse:
        assert context is not None
        emitted = []
        for number in range(request.results_wanted):
            job = _job(number)
            if context.emit_job(job, {"index": number + 1}):
                emitted.append(job)
        return JobResponse(jobs=emitted)


def _registry(
    adapter: Callable[..., Scraper] = _RestartableAdapter,
) -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(Site.INDEED, adapter)
    return registry


def _write_with_key(
    checkpoint: SearchCheckpoint,
    key: str,
) -> CheckpointWrite:
    adapter = checkpoint.adapters[Site.INDEED.value].model_copy(
        update={
            "emitted_count": 1,
            "state": {"index": 1},
        }
    )
    updated = checkpoint.model_copy(
        update={
            "revision": checkpoint.revision + 1,
            "adapters": {Site.INDEED.value: adapter},
        }
    )
    return CheckpointWrite(
        checkpoint=updated,
        adapter_site=Site.INDEED,
        new_seen_job_key=key,
    )


def test_sqlite_store_round_trips_and_clears_a_full_checkpoint(tmp_path) -> None:
    path = tmp_path / "nested" / "checkpoint.sqlite3"
    store = SqliteCheckpointStore(path)
    request = SearchRequest(site_type=(Site.INDEED,), search_term="python")
    initial = SearchCheckpoint.for_request(request)
    store.save(initial)

    adapter = initial.adapters[Site.INDEED.value].model_copy(
        update={
            "state": {"cursor": "next"},
            "seen_job_keys": ("key-1", "key-2"),
            "emitted_count": 2,
        }
    )
    updated = initial.model_copy(
        update={
            "revision": 1,
            "adapters": {Site.INDEED.value: adapter},
        }
    )
    store.save(updated)

    reopened = SqliteCheckpointStore(path)
    assert reopened.load() == updated
    assert reopened.stats().revision == 1
    assert reopened.stats().adapter_count == 1
    assert reopened.stats().seen_job_key_count == 2

    reopened.clear()
    assert reopened.load() is None
    assert reopened.stats().seen_job_key_count == 0


def test_sqlite_store_closes_every_connection_before_return(
    monkeypatch,
    tmp_path,
) -> None:
    connections = []
    connect = sqlite3.connect

    class TrackingConnection(sqlite3.Connection):
        closed = False

        def close(self) -> None:
            self.closed = True
            super().close()

    def tracking_connect(*args, **kwargs):
        connection = connect(*args, factory=TrackingConnection, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(
        "jobstreaming.checkpoint.sqlite3.connect",
        tracking_connect,
    )

    path = tmp_path / "checkpoint.sqlite3"
    store = SqliteCheckpointStore(path)
    checkpoint = SearchCheckpoint.for_request(SearchRequest(site_type=(Site.INDEED,)))
    store.save(checkpoint)
    assert store.load() == checkpoint
    stats = store.stats()

    assert connections
    assert all(connection.closed for connection in connections)
    assert stats.database_bytes == path.stat().st_size


def test_sqlite_store_closes_connection_when_setup_pragma_fails(
    monkeypatch,
    tmp_path,
) -> None:
    connections = []
    connect = sqlite3.connect

    class FailingSetupConnection(sqlite3.Connection):
        closed = False

        def execute(self, sql, parameters=()):
            if "PRAGMA foreign_keys" in sql:
                raise sqlite3.OperationalError("forced setup failure")
            return super().execute(sql, parameters)

        def close(self) -> None:
            self.closed = True
            super().close()

    def failing_connect(*args, **kwargs):
        connection = connect(*args, factory=FailingSetupConnection, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(
        "jobstreaming.checkpoint.sqlite3.connect",
        failing_connect,
    )

    with pytest.raises(CheckpointError, match="forced setup failure"):
        SqliteCheckpointStore(tmp_path / "checkpoint.sqlite3")

    assert len(connections) == 1
    assert connections[0].closed is True


def test_future_sqlite_schema_is_rejected_without_adding_current_tables(
    tmp_path,
) -> None:
    path = tmp_path / "future.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE jobstreaming_checkpoint_schema (
                singleton INTEGER PRIMARY KEY,
                version INTEGER NOT NULL
            );
            INSERT INTO jobstreaming_checkpoint_schema VALUES (1, 2);
            CREATE TABLE future_only (value TEXT);
            """)
        before = tuple(row[0] for row in connection.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                ORDER BY name
                """))

    with pytest.raises(CheckpointCompatibilityError):
        SqliteCheckpointStore(path)

    with sqlite3.connect(path) as connection:
        after = tuple(row[0] for row in connection.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                ORDER BY name
                """))
    assert after == before


def test_sqlite_incremental_writes_preserve_acknowledgement_order(tmp_path) -> None:
    store = SqliteCheckpointStore(tmp_path / "checkpoint.sqlite3")
    request = SearchRequest(site_type=(Site.INDEED,))
    checkpoint = SearchCheckpoint.for_request(request)
    store.save(checkpoint)

    for position, key in enumerate(("key-a", "key-b", "key-c"), start=1):
        adapter = checkpoint.adapters[Site.INDEED.value].model_copy(
            update={
                "emitted_count": position,
                "state": {"position": position},
            }
        )
        checkpoint = checkpoint.model_copy(
            update={
                "revision": position,
                "adapters": {Site.INDEED.value: adapter},
            }
        )
        store.save_incremental(
            CheckpointWrite(
                checkpoint=checkpoint,
                adapter_site=Site.INDEED,
                new_seen_job_key=key,
            )
        )

    loaded = store.load()
    assert loaded is not None
    adapter = loaded.adapters[Site.INDEED.value]
    assert adapter.seen_job_keys == ("key-a", "key-b", "key-c")
    assert adapter.emitted_count == 3
    assert adapter.state == {"position": 3}


def test_sqlite_compare_and_swap_allows_only_one_concurrent_owner(
    tmp_path,
) -> None:
    path = tmp_path / "checkpoint.sqlite3"
    request = SearchRequest(site_type=(Site.INDEED,))
    initial = SearchCheckpoint.for_request(request)
    SqliteCheckpointStore(path).save(initial)
    stores = [SqliteCheckpointStore(path), SqliteCheckpointStore(path)]
    writes = [
        _write_with_key(initial, "owner-a"),
        _write_with_key(initial, "owner-b"),
    ]
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def save(store: SqliteCheckpointStore, write: CheckpointWrite) -> None:
        barrier.wait(timeout=1)
        try:
            store.save_incremental(write)
        except CheckpointConflictError:
            outcomes.append("conflict")
        else:
            outcomes.append("saved")

    threads = [
        threading.Thread(target=save, args=(store, write))
        for store, write in zip(stores, writes, strict=True)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(outcomes) == ["conflict", "saved"]
    loaded = SqliteCheckpointStore(path).load()
    assert loaded is not None
    assert loaded.revision == 1
    assert len(loaded.adapters[Site.INDEED.value].seen_job_keys) == 1


def test_sqlite_load_uses_one_snapshot_across_all_checkpoint_tables(
    tmp_path,
) -> None:
    path = tmp_path / "checkpoint.sqlite3"
    initial = SearchCheckpoint.for_request(SearchRequest(site_type=(Site.INDEED,)))
    writer = SqliteCheckpointStore(path)
    writer.save(initial)
    write = _write_with_key(initial, "committed-after-header")
    header_read = threading.Event()
    writer_committed = threading.Event()
    writer_errors: list[BaseException] = []

    class InterleavingReader(SqliteCheckpointStore):
        def __init__(self, checkpoint_path) -> None:
            self.pause_reads = False
            super().__init__(checkpoint_path)
            self.pause_reads = True

        def _connect(self):
            connection = super()._connect()
            if not self.pause_reads:
                return connection

            def pause_after_header(sql: str) -> None:
                normalized = " ".join(sql.upper().split())
                if (
                    self.pause_reads
                    and "FROM JOBSTREAMING_SEEN_JOB_KEY" in normalized
                    and "ORDER BY SITE, POSITION" in normalized
                ):
                    self.pause_reads = False
                    header_read.set()
                    writer_committed.wait(timeout=2)

            connection.set_trace_callback(pause_after_header)
            return connection

    def commit_after_header() -> None:
        if not header_read.wait(timeout=2):
            writer_errors.append(RuntimeError("reader did not reach snapshot window"))
            writer_committed.set()
            return
        try:
            writer.save_incremental(write)
        except BaseException as exc:
            writer_errors.append(exc)
        finally:
            writer_committed.set()

    writer_thread = threading.Thread(target=commit_after_header)
    writer_thread.start()
    loaded = InterleavingReader(path).load()
    writer_thread.join(timeout=2)

    assert writer_thread.is_alive() is False
    assert writer_committed.is_set()
    assert writer_errors == []
    assert loaded == initial

    committed = writer.load()
    assert committed is not None
    assert committed.revision == 1
    assert committed.adapters[Site.INDEED.value].seen_job_keys == (
        "committed-after-header",
    )


def test_sqlite_stream_replays_only_the_unacknowledged_tail(tmp_path) -> None:
    path = tmp_path / "checkpoint.sqlite3"
    store = SqliteCheckpointStore(path)
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=3)
    first_stream = stream_search(
        request,
        registry=_registry(),
        checkpoint_store=store,
        ack_mode="explicit",
    )
    first = next(first_stream)
    assert isinstance(first, JobEvent)
    first_stream.ack(first)
    assert first_stream.checkpoint.adapters[Site.INDEED.value].seen_job_keys == (
        first.job_key,
    )

    second = next(first_stream)
    assert isinstance(second, JobEvent)
    first_stream.close()
    assert first_stream.wait_closed(1).quiescent is True

    restarted = SqliteCheckpointStore(path)
    with stream_search(
        request,
        registry=_registry(),
        checkpoint_store=restarted,
        ack_mode="explicit",
    ) as resumed:
        replayed = []
        for event in resumed:
            if isinstance(event, JobEvent):
                replayed.append(event.job.id)
            resumed.ack(event)

    assert replayed == ["job-1", "job-2"]
    assert restarted.stats().seen_job_key_count == 3


def test_runtime_does_not_copy_loaded_seen_history_for_each_sqlite_ack(
    tmp_path,
) -> None:
    recorded_snapshot_lengths: list[int] = []

    class RecordingStore(SqliteCheckpointStore):
        def save_incremental(self, write: CheckpointWrite) -> None:
            if write.adapter_site is not None:
                recorded_snapshot_lengths.append(
                    len(
                        write.checkpoint.adapters[
                            write.adapter_site.value
                        ].seen_job_keys
                    )
                )
            super().save_incremental(write)

    store = RecordingStore(tmp_path / "checkpoint.sqlite3")
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=250)
    with stream_search(
        request,
        registry=_registry(),
        checkpoint_store=store,
        ack_mode="explicit",
    ) as stream:
        for event in stream:
            stream.ack(event)

    assert store.stats().seen_job_key_count == 250
    assert recorded_snapshot_lengths
    assert set(recorded_snapshot_lengths) == {0}


def test_incremental_seen_key_table_is_append_only(tmp_path) -> None:
    path = tmp_path / "checkpoint.sqlite3"
    store = SqliteCheckpointStore(path)
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=5)
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TRIGGER forbid_seen_key_update
            BEFORE UPDATE ON jobstreaming_seen_job_key
            BEGIN
                SELECT RAISE(FAIL, 'seen keys are append-only');
            END;

            CREATE TRIGGER forbid_seen_key_delete
            BEFORE DELETE ON jobstreaming_seen_job_key
            BEGIN
                SELECT RAISE(FAIL, 'seen keys are append-only');
            END;
            """)

    with stream_search(
        request,
        registry=_registry(),
        checkpoint_store=store,
        ack_mode="explicit",
    ) as stream:
        for event in stream:
            stream.ack(event)

    assert store.stats().seen_job_key_count == 5


def test_resume_false_transactionally_replaces_existing_sqlite_search(
    tmp_path,
) -> None:
    store = SqliteCheckpointStore(tmp_path / "checkpoint.sqlite3")
    original = SearchRequest(
        site_type=(Site.INDEED,),
        search_term="python",
        results_wanted=1,
    )
    with stream_search(
        original,
        registry=_registry(),
        checkpoint_store=store,
    ) as stream:
        list(stream)

    replacement = SearchRequest(
        site_type=(Site.INDEED,),
        search_term="rust",
        results_wanted=1,
    )
    with stream_search(
        replacement,
        registry=_registry(),
        checkpoint_store=store,
        resume=False,
    ) as stream:
        list(stream)

    loaded = store.load()
    assert loaded is not None
    assert loaded.request_fingerprint == replacement.fingerprint()
    assert loaded.adapters[Site.INDEED.value].seen_job_keys


def test_sqlite_replace_without_existing_row_returns_initial_checkpoint(
    tmp_path,
) -> None:
    store = SqliteCheckpointStore(tmp_path / "checkpoint.sqlite3")
    checkpoint = SearchCheckpoint.for_request(
        SearchRequest(site_type=(Site.INDEED,), search_term="python")
    )

    persisted = store.replace(checkpoint)

    assert persisted == checkpoint
    assert persisted.revision == 0
    assert store.load() == persisted


def test_resume_false_fences_a_stale_owner_of_the_same_sqlite_search(
    tmp_path,
) -> None:
    store = SqliteCheckpointStore(tmp_path / "checkpoint.sqlite3")
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=2)
    stale_stream = stream_search(
        request,
        registry=_registry(),
        checkpoint_store=store,
        ack_mode="explicit",
    )
    stale_event = next(stale_stream)
    assert isinstance(stale_event, JobEvent)

    replacement_stream = stream_search(
        request,
        registry=_registry(),
        checkpoint_store=store,
        resume=False,
        ack_mode="explicit",
    )
    replacement_event = next(replacement_stream)
    assert isinstance(replacement_event, JobEvent)
    assert replacement_stream.checkpoint.revision == 1

    with pytest.raises(CheckpointConflictError):
        stale_stream.ack(stale_event)

    replacement_stream.ack(replacement_event)
    assert replacement_stream.checkpoint.revision == 2
    persisted = store.load()
    assert persisted is not None
    assert persisted.revision == 2
    assert persisted.adapters[Site.INDEED.value].seen_job_keys == (
        replacement_event.job_key,
    )

    stale_stream.close()
    replacement_stream.close()
    assert stale_stream.wait_closed(1).quiescent is True
    assert replacement_stream.wait_closed(1).quiescent is True


def test_resume_false_reseed_failure_preserves_existing_sqlite_search(
    tmp_path,
) -> None:
    path = tmp_path / "checkpoint.sqlite3"
    store = SqliteCheckpointStore(path)
    original = SearchRequest(
        site_type=(Site.INDEED,),
        search_term="python",
        results_wanted=1,
    )
    with stream_search(
        original,
        registry=_registry(),
        checkpoint_store=store,
    ) as stream:
        list(stream)
    before = store.load()
    assert before is not None

    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TRIGGER fail_replacement_adapter_insert
            BEFORE INSERT ON jobstreaming_adapter_checkpoint
            BEGIN
                SELECT RAISE(ABORT, 'forced replacement insert failure');
            END;
            """)

    replacement = SearchRequest(
        site_type=(Site.INDEED,),
        search_term="rust",
        results_wanted=1,
    )
    with pytest.raises(
        CheckpointError,
        match="forced replacement insert failure",
    ):
        stream_search(
            replacement,
            registry=_registry(),
            checkpoint_store=store,
            resume=False,
        )

    assert store.load() == before


@pytest.mark.parametrize(
    "write",
    [
        lambda checkpoint: CheckpointWrite(checkpoint=checkpoint),
        lambda checkpoint: CheckpointWrite(
            checkpoint=checkpoint.model_copy(update={"revision": 1}),
            new_seen_job_key="key",
        ),
    ],
)
def test_checkpoint_write_rejects_illegal_states(write, tmp_path) -> None:
    request = SearchRequest(site_type=(Site.INDEED,))
    checkpoint = SearchCheckpoint.for_request(request)

    with pytest.raises(ValueError):
        write(checkpoint)


def test_checkpoint_benchmark_scenario_verifies_storage_facts(tmp_path) -> None:
    result = run_scenario(25, tmp_path / "benchmark.sqlite3")

    assert result["acknowledgements"] == 25
    assert result["final_revision"] == 25
    assert result["seen_job_keys"] == 25
    assert result["database_bytes"] > 0
