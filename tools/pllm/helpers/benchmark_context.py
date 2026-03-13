from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import os

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


def append_event(
    log_path: str | os.PathLike[str] | None,
    kind: str,
    message: str,
    *,
    snippet: str = "",
    step: str = "",
) -> None:
    if not log_path:
        return

    path = Path(log_path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).isoformat()
        header = f"===== {timestamp} kind={kind}"
        if snippet:
            header += f" snippet={snippet}"
        if step:
            header += f" step={step}"
        header += " =====\n"
        payload = message.rstrip()
        if payload:
            payload += "\n"
        block = f"{header}{payload}\n"
        with path.open("a", encoding="utf-8") as handle:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            handle.write(block)
            handle.flush()
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        return


def read_tail(
    log_path: str | os.PathLike[str] | None,
    max_bytes: int = 48_000,
) -> str:
    if not log_path:
        return ""

    path = Path(log_path)
    if not path.exists():
        return ""

    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(size - max_bytes, 0))
            data = handle.read()
        text = data.decode("utf-8", errors="replace").strip()
        if not text:
            return ""
        if size > max_bytes:
            return "[older benchmark context omitted]\n" + text
        return text
    except OSError:
        return ""
