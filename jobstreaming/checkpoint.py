from __future__ import annotations

import json
import math
import os
import sqlite3
import tempfile
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Protocol, runtime_checkable

from jobstreaming.events import (
    LEGACY_CHECKPOINT_GENERATION,
    AdapterCheckpoint,
    SearchCheckpoint,
)
from jobstreaming.model import AdapterIdentifier


class CheckpointError(RuntimeError):
    pass


class CheckpointMismatchError(CheckpointError):
    pass


class CheckpointCompatibilityError(CheckpointError):
    """The checkpoint or an adapter cursor uses an unsupported schema version."""


class CheckpointConflictError(CheckpointError):
    """A store rejected a save because the caller has stale checkpoint ownership."""


class CheckpointStore(Protocol):
    def load(self) -> SearchCheckpoint | None: ...

    def save(self, checkpoint: SearchCheckpoint) -> None: ...

    def clear(self) -> None: ...


@runtime_checkable
class AtomicCheckpointStore(CheckpointStore, Protocol):
    """A store that can replace one search checkpoint as a single transition."""

    def replace(self, checkpoint: SearchCheckpoint) -> SearchCheckpoint: ...


@dataclass(frozen=True, slots=True)
class CheckpointWrite:
    """One acknowledged checkpoint transition and its append-only key delta."""

    checkpoint: SearchCheckpoint
    adapter_site: AdapterIdentifier | None = None
    new_seen_job_key: str | None = None

    def __post_init__(self) -> None:
        if self.checkpoint.revision < 1:
            raise ValueError("incremental checkpoint writes require revision >= 1")
        if self.new_seen_job_key is not None and self.adapter_site is None:
            raise ValueError("a seen job key requires an adapter site")
        if self.new_seen_job_key is not None and not self.new_seen_job_key:
            raise ValueError("seen job keys cannot be empty")
        if self.adapter_site is None:
            return
        adapter = self.checkpoint.adapters.get(self.adapter_site.value)
        if adapter is None or adapter.site != self.adapter_site:
            raise ValueError("adapter_site must identify a checkpoint adapter")
        if self.new_seen_job_key is not None and adapter.emitted_count < 1:
            raise ValueError("a seen job key requires a positive emitted count")


@runtime_checkable
class IncrementalCheckpointStore(CheckpointStore, Protocol):
    """A store that can persist one checkpoint transition without a full rewrite."""

    def save_incremental(self, write: CheckpointWrite) -> None: ...


@dataclass(frozen=True, slots=True)
class CheckpointStorageStats:
    revision: int | None
    adapter_count: int
    seen_job_key_count: int
    database_bytes: int


class MemoryCheckpointStore:
    def __init__(self) -> None:
        self._checkpoint: SearchCheckpoint | None = None
        self._lock = Lock()

    def load(self) -> SearchCheckpoint | None:
        with self._lock:
            if self._checkpoint is None:
                return None
            return self._checkpoint.model_copy(deep=True)

    def save(self, checkpoint: SearchCheckpoint) -> None:
        with self._lock:
            self._checkpoint = checkpoint.model_copy(deep=True)

    def clear(self) -> None:
        with self._lock:
            self._checkpoint = None

    def replace(self, checkpoint: SearchCheckpoint) -> SearchCheckpoint:
        self.save(checkpoint)
        return checkpoint.model_copy(deep=True)


class JsonFileCheckpointStore:
    """Atomic JSON checkpoint storage for a single search stream."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        self._lock = Lock()

    def load(self) -> SearchCheckpoint | None:
        with self._lock:
            if not self.path.exists():
                return None
            try:
                return SearchCheckpoint.model_validate_json(
                    self.path.read_text(encoding="utf-8")
                )
            except Exception as exc:
                raise CheckpointError(
                    f"Unable to read checkpoint {self.path}: {exc}"
                ) from exc

    def save(self, checkpoint: SearchCheckpoint) -> None:
        payload = checkpoint.model_dump_json(indent=2)
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary_name: str | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    dir=self.path.parent,
                    prefix=f".{self.path.name}.",
                    suffix=".tmp",
                    delete=False,
                ) as handle:
                    temporary_name = handle.name
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary_name, self.path)
            finally:
                if temporary_name and os.path.exists(temporary_name):
                    os.unlink(temporary_name)

    def clear(self) -> None:
        with self._lock:
            self.path.unlink(missing_ok=True)

    def replace(self, checkpoint: SearchCheckpoint) -> SearchCheckpoint:
        self.save(checkpoint)
        return checkpoint.model_copy(deep=True)


class SqliteCheckpointStore:
    """Transactional SQLite checkpoint storage for one search stream.

    Checkpoint metadata and adapter state use generation-and-revision
    compare-and-swap semantics. Seen job keys live in an ordered append-only table, so
    an acknowledgement writes only its new key rather than replacing all historical
    keys.
    """

    _SCHEMA_VERSION = 1

    def __init__(self, path: str | Path, *, timeout: float = 5.0) -> None:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise ValueError("timeout must be a finite, non-negative number")
        self.path = Path(path).expanduser().resolve()
        self.timeout = float(timeout)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.timeout,
            isolation_level=None,
        )
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(f"PRAGMA busy_timeout = {int(self.timeout * 1_000)}")
            connection.execute("PRAGMA synchronous = FULL")
            return connection
        except BaseException:
            connection.close()
            raise

    def _validate_existing_schema(self, connection: sqlite3.Connection) -> bool:
        schema_table = connection.execute("""
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'jobstreaming_checkpoint_schema'
            """).fetchone()
        if schema_table is None:
            return False

        version = connection.execute("""
            SELECT version
            FROM jobstreaming_checkpoint_schema
            WHERE singleton = 1
            """).fetchone()
        if version is None or version["version"] != self._SCHEMA_VERSION:
            stored_version = version["version"] if version is not None else "missing"
            raise CheckpointCompatibilityError(
                f"SQLite checkpoint schema {stored_version} is incompatible with "
                f"schema {self._SCHEMA_VERSION}"
            )
        return True

    @staticmethod
    def _migrate_v1_generation(connection: sqlite3.Connection) -> None:
        search_table = connection.execute("""
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'jobstreaming_search_checkpoint'
            """).fetchone()
        if search_table is None:
            return
        columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(jobstreaming_search_checkpoint)"
            )
        }
        if "generation" in columns:
            return
        connection.execute(f"""
            ALTER TABLE jobstreaming_search_checkpoint
            ADD COLUMN generation TEXT NOT NULL
                DEFAULT '{LEGACY_CHECKPOINT_GENERATION}'
            """)

    def _initialize(self) -> None:
        try:
            with closing(self._connect()) as connection:
                has_supported_schema = self._validate_existing_schema(connection)
                if has_supported_schema:
                    connection.execute("BEGIN IMMEDIATE")
                    try:
                        self._migrate_v1_generation(connection)
                        connection.commit()
                    except Exception:
                        connection.rollback()
                        raise
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA synchronous = FULL")
                connection.executescript("""
                    CREATE TABLE IF NOT EXISTS jobstreaming_checkpoint_schema (
                        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                        version INTEGER NOT NULL CHECK (version >= 1)
                    );

                    INSERT OR IGNORE INTO jobstreaming_checkpoint_schema
                        (singleton, version)
                    VALUES (1, 1);

                    CREATE TABLE IF NOT EXISTS jobstreaming_search_checkpoint (
                        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                        version INTEGER NOT NULL CHECK (version >= 1),
                        revision INTEGER NOT NULL CHECK (revision >= 0),
                        generation TEXT NOT NULL CHECK (length(generation) > 0),
                        request_fingerprint TEXT NOT NULL,
                        completed INTEGER NOT NULL CHECK (completed IN (0, 1)),
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS jobstreaming_adapter_checkpoint (
                        site TEXT PRIMARY KEY,
                        cursor_schema_version INTEGER NOT NULL
                            CHECK (cursor_schema_version >= 1),
                        state_json TEXT NOT NULL,
                        emitted_count INTEGER NOT NULL CHECK (emitted_count >= 0),
                        completed INTEGER NOT NULL CHECK (completed IN (0, 1)),
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS jobstreaming_seen_job_key (
                        site TEXT NOT NULL,
                        job_key TEXT NOT NULL CHECK (length(job_key) > 0),
                        position INTEGER NOT NULL CHECK (position >= 1),
                        PRIMARY KEY (site, job_key),
                        UNIQUE (site, position),
                        FOREIGN KEY (site) REFERENCES
                            jobstreaming_adapter_checkpoint(site)
                            ON DELETE CASCADE
                    ) WITHOUT ROWID;
                    """)
        except CheckpointCompatibilityError:
            raise
        except sqlite3.Error as exc:
            raise CheckpointError(
                f"Unable to initialize SQLite checkpoint {self.path}: {exc}"
            ) from exc

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _read_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _adapter_state_json(adapter: AdapterCheckpoint) -> str:
        return json.dumps(
            adapter.model_dump(mode="json")["state"],
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    @staticmethod
    def _header_values(checkpoint: SearchCheckpoint) -> tuple[object, ...]:
        return (
            checkpoint.version,
            checkpoint.revision,
            checkpoint.generation,
            checkpoint.request_fingerprint,
            int(checkpoint.completed),
            checkpoint.updated_at.isoformat(),
        )

    def _insert_header(
        self,
        connection: sqlite3.Connection,
        checkpoint: SearchCheckpoint,
    ) -> None:
        connection.execute(
            """
            INSERT INTO jobstreaming_search_checkpoint (
                singleton,
                version,
                revision,
                generation,
                request_fingerprint,
                completed,
                updated_at
            ) VALUES (1, ?, ?, ?, ?, ?, ?)
            """,
            self._header_values(checkpoint),
        )

    def _advance_header(
        self,
        connection: sqlite3.Connection,
        checkpoint: SearchCheckpoint,
    ) -> bool:
        current = connection.execute("""
            SELECT version, revision, generation, request_fingerprint
            FROM jobstreaming_search_checkpoint
            WHERE singleton = 1
            """).fetchone()
        if current is None:
            if checkpoint.revision != 0:
                raise CheckpointConflictError(
                    "initial SQLite checkpoint revision must be zero"
                )
            self._insert_header(connection, checkpoint)
            return True

        expected_revision = int(current["revision"]) + 1
        if checkpoint.revision != expected_revision:
            raise CheckpointConflictError(
                "SQLite checkpoint compare-and-swap conflict: "
                f"expected revision {expected_revision}, "
                f"received {checkpoint.revision}"
            )
        if (
            current["version"] != checkpoint.version
            or current["generation"] != checkpoint.generation
            or current["request_fingerprint"] != checkpoint.request_fingerprint
        ):
            raise CheckpointConflictError(
                "SQLite checkpoint identity changed during compare-and-swap"
            )
        updated = connection.execute(
            """
            UPDATE jobstreaming_search_checkpoint
            SET version = ?,
                revision = ?,
                generation = ?,
                request_fingerprint = ?,
                completed = ?,
                updated_at = ?
            WHERE singleton = 1
              AND revision = ?
              AND generation = ?
            """,
            self._header_values(checkpoint)
            + (current["revision"], current["generation"]),
        )
        if updated.rowcount != 1:
            raise CheckpointConflictError(
                "SQLite checkpoint compare-and-swap update lost ownership"
            )
        return False

    def _write_adapter(
        self,
        connection: sqlite3.Connection,
        adapter: AdapterCheckpoint,
    ) -> None:
        connection.execute(
            """
            INSERT INTO jobstreaming_adapter_checkpoint (
                site,
                cursor_schema_version,
                state_json,
                emitted_count,
                completed,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(site) DO UPDATE SET
                cursor_schema_version = excluded.cursor_schema_version,
                state_json = excluded.state_json,
                emitted_count = excluded.emitted_count,
                completed = excluded.completed,
                updated_at = excluded.updated_at
            """,
            (
                adapter.site.value,
                adapter.cursor_schema_version,
                self._adapter_state_json(adapter),
                adapter.emitted_count,
                int(adapter.completed),
                adapter.updated_at.isoformat(),
            ),
        )

    def _save_full(
        self,
        connection: sqlite3.Connection,
        checkpoint: SearchCheckpoint,
    ) -> None:
        stored_sites = {
            row["site"]
            for row in connection.execute(
                "SELECT site FROM jobstreaming_adapter_checkpoint"
            )
        }
        incoming_sites = set(checkpoint.adapters)
        if not stored_sites.issubset(incoming_sites):
            raise CheckpointConflictError(
                "SQLite checkpoints cannot remove adapter history; clear first"
            )

        for site_name, adapter in checkpoint.adapters.items():
            if site_name != adapter.site.value:
                raise CheckpointError(
                    f"Checkpoint key {site_name!r} does not match adapter site"
                )
            if adapter.emitted_count != len(adapter.seen_job_keys):
                raise CheckpointError(
                    f"{site_name} emitted_count must equal its seen key count"
                )
            stored_keys = {
                row["job_key"]: row["position"]
                for row in connection.execute(
                    """
                    SELECT job_key, position
                    FROM jobstreaming_seen_job_key
                    WHERE site = ?
                    """,
                    (site_name,),
                )
            }
            incoming_keys = set(adapter.seen_job_keys)
            if not set(stored_keys).issubset(incoming_keys):
                raise CheckpointConflictError(
                    f"{site_name} acknowledged keys are append-only; clear first"
                )
            self._write_adapter(connection, adapter)
            for position, job_key in enumerate(adapter.seen_job_keys, start=1):
                stored_position = stored_keys.get(job_key)
                if stored_position is not None:
                    if stored_position != position:
                        raise CheckpointConflictError(
                            f"{site_name} seen key order changed"
                        )
                    continue
                connection.execute(
                    """
                    INSERT INTO jobstreaming_seen_job_key (
                        site,
                        job_key,
                        position
                    ) VALUES (?, ?, ?)
                    """,
                    (site_name, job_key, position),
                )

    def load(self) -> SearchCheckpoint | None:
        try:
            with self._read_transaction() as connection:
                header = connection.execute("""
                    SELECT
                        version,
                        revision,
                        generation,
                        request_fingerprint,
                        completed,
                        updated_at
                    FROM jobstreaming_search_checkpoint
                    WHERE singleton = 1
                    """).fetchone()
                if header is None:
                    return None

                keys_by_site: dict[str, list[str]] = {}
                for row in connection.execute("""
                    SELECT site, job_key
                    FROM jobstreaming_seen_job_key
                    ORDER BY site, position
                    """):
                    keys_by_site.setdefault(row["site"], []).append(row["job_key"])

                adapters: dict[str, AdapterCheckpoint] = {}
                for row in connection.execute("""
                    SELECT
                        site,
                        cursor_schema_version,
                        state_json,
                        emitted_count,
                        completed,
                        updated_at
                    FROM jobstreaming_adapter_checkpoint
                    ORDER BY site
                    """):
                    site_name = str(row["site"])
                    seen_job_keys = tuple(keys_by_site.get(site_name, ()))
                    if row["emitted_count"] != len(seen_job_keys):
                        raise ValueError(
                            f"{site_name} emitted count does not match seen keys"
                        )
                    adapters[site_name] = AdapterCheckpoint.model_validate(
                        {
                            "site": site_name,
                            "cursor_schema_version": row["cursor_schema_version"],
                            "state": json.loads(row["state_json"]),
                            "seen_job_keys": seen_job_keys,
                            "emitted_count": row["emitted_count"],
                            "completed": bool(row["completed"]),
                            "updated_at": row["updated_at"],
                        }
                    )
                return SearchCheckpoint.model_validate(
                    {
                        "version": header["version"],
                        "revision": header["revision"],
                        "generation": header["generation"],
                        "request_fingerprint": header["request_fingerprint"],
                        "adapters": adapters,
                        "completed": bool(header["completed"]),
                        "updated_at": header["updated_at"],
                    }
                )
        except (json.JSONDecodeError, ValueError, sqlite3.Error) as exc:
            raise CheckpointError(
                f"Unable to read SQLite checkpoint {self.path}: {exc}"
            ) from exc

    def save(self, checkpoint: SearchCheckpoint) -> None:
        try:
            with self._transaction() as connection:
                self._advance_header(connection, checkpoint)
                self._save_full(connection, checkpoint)
        except (CheckpointConflictError, CheckpointError):
            raise
        except sqlite3.Error as exc:
            raise CheckpointError(
                f"Unable to save SQLite checkpoint {self.path}: {exc}"
            ) from exc

    def save_incremental(self, write: CheckpointWrite) -> None:
        checkpoint = write.checkpoint
        try:
            with self._transaction() as connection:
                self._advance_header(connection, checkpoint)
                if write.adapter_site is None:
                    return

                site_name = write.adapter_site.value
                adapter = checkpoint.adapters[site_name]
                stored = connection.execute(
                    """
                    SELECT emitted_count
                    FROM jobstreaming_adapter_checkpoint
                    WHERE site = ?
                    """,
                    (site_name,),
                ).fetchone()
                previous_count = (
                    int(stored["emitted_count"]) if stored is not None else 0
                )
                expected_count = previous_count + (
                    1 if write.new_seen_job_key is not None else 0
                )
                if adapter.emitted_count != expected_count:
                    raise CheckpointConflictError(
                        f"{site_name} emitted count changed non-incrementally: "
                        f"expected {expected_count}, "
                        f"received {adapter.emitted_count}"
                    )
                self._write_adapter(connection, adapter)
                if write.new_seen_job_key is not None:
                    connection.execute(
                        """
                        INSERT INTO jobstreaming_seen_job_key (
                            site,
                            job_key,
                            position
                        ) VALUES (?, ?, ?)
                        """,
                        (
                            site_name,
                            write.new_seen_job_key,
                            adapter.emitted_count,
                        ),
                    )
        except (CheckpointConflictError, CheckpointError):
            raise
        except sqlite3.IntegrityError as exc:
            raise CheckpointConflictError(
                f"SQLite checkpoint seen-key conflict: {exc}"
            ) from exc
        except sqlite3.Error as exc:
            raise CheckpointError(
                f"Unable to increment SQLite checkpoint {self.path}: {exc}"
            ) from exc

    @staticmethod
    def _clear_checkpoint(connection: sqlite3.Connection) -> None:
        connection.execute("DELETE FROM jobstreaming_seen_job_key")
        connection.execute("DELETE FROM jobstreaming_adapter_checkpoint")
        connection.execute(
            "DELETE FROM jobstreaming_search_checkpoint WHERE singleton = 1"
        )

    def clear(self) -> None:
        try:
            with self._transaction() as connection:
                self._clear_checkpoint(connection)
        except sqlite3.Error as exc:
            raise CheckpointError(
                f"Unable to clear SQLite checkpoint {self.path}: {exc}"
            ) from exc

    def replace(self, checkpoint: SearchCheckpoint) -> SearchCheckpoint:
        try:
            with self._transaction() as connection:
                current = connection.execute("""
                    SELECT revision
                    FROM jobstreaming_search_checkpoint
                    WHERE singleton = 1
                    """).fetchone()
                if current is None:
                    if checkpoint.revision != 0:
                        raise CheckpointConflictError(
                            "initial SQLite checkpoint revision must be zero"
                        )
                    persisted = checkpoint
                else:
                    persisted = checkpoint.model_copy(
                        update={"revision": int(current["revision"]) + 1}
                    )
                self._clear_checkpoint(connection)
                self._insert_header(connection, persisted)
                self._save_full(connection, persisted)
                return persisted
        except (CheckpointConflictError, CheckpointError):
            raise
        except sqlite3.Error as exc:
            raise CheckpointError(
                f"Unable to replace SQLite checkpoint {self.path}: {exc}"
            ) from exc

    def stats(self) -> CheckpointStorageStats:
        try:
            with self._read_transaction() as connection:
                header = connection.execute("""
                    SELECT revision
                    FROM jobstreaming_search_checkpoint
                    WHERE singleton = 1
                    """).fetchone()
                adapter_count = connection.execute(
                    "SELECT COUNT(*) FROM jobstreaming_adapter_checkpoint"
                ).fetchone()[0]
                seen_count = connection.execute(
                    "SELECT COUNT(*) FROM jobstreaming_seen_job_key"
                ).fetchone()[0]
            return CheckpointStorageStats(
                revision=int(header["revision"]) if header is not None else None,
                adapter_count=int(adapter_count),
                seen_job_key_count=int(seen_count),
                database_bytes=self.path.stat().st_size if self.path.exists() else 0,
            )
        except sqlite3.Error as exc:
            raise CheckpointError(
                f"Unable to inspect SQLite checkpoint {self.path}: {exc}"
            ) from exc
