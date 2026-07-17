from __future__ import annotations

import os
import tempfile
from pathlib import Path
from threading import Lock
from typing import Protocol

from jobstreaming.events import SearchCheckpoint


class CheckpointError(RuntimeError):
    pass


class CheckpointMismatchError(CheckpointError):
    pass


class CheckpointStore(Protocol):
    def load(self) -> SearchCheckpoint | None: ...

    def save(self, checkpoint: SearchCheckpoint) -> None: ...

    def clear(self) -> None: ...


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
