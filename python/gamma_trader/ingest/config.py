from __future__ import annotations

import os
import platform
from pathlib import Path
from typing import Any


def resolve_snapshot_dir(cfg: dict[str, Any]) -> Path:
    """Resolve snapshot_dir from config.

    Supports:
    - snapshot_dir: "/mnt/SPX"
    - snapshot_dir: { linux: "/mnt/SPX", windows: "C:/..." }
    """
    snap = cfg.get("snapshot_dir")
    if isinstance(snap, str):
        return Path(snap)

    if isinstance(snap, dict):
        sys = platform.system().lower()  # 'windows', 'linux', 'darwin'
        if sys in snap:
            return Path(snap[sys])
        # fallback env override
        if os.getenv("GT_SNAPSHOT_DIR"):
            return Path(os.environ["GT_SNAPSHOT_DIR"])
        # any first value
        for v in snap.values():
            if isinstance(v, str) and v:
                return Path(v)

    raise ValueError("config must define snapshot_dir")
