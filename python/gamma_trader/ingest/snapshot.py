import json
import re
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Any


_SNAPSHOT_RE = re.compile(
    r"^(?P<ticker>[A-Z]+)-(?P<spot>\d+(?:\.\d+)?)-(?P<expY>\d{4})-(?P<expM>\d{2})-(?P<expD>\d{2})-"
    r"(?P<obsDate>\d{8})-(?P<obsTime>\d{6})\.json$"
)


@dataclass(frozen=True)
class SnapshotMeta:
    ticker: str
    spot_in_name: float
    expiration: date
    observed_dt: datetime


def parse_snapshot_filename(name: str) -> SnapshotMeta | None:
    m = _SNAPSHOT_RE.match(name)
    if not m:
        return None
    exp = date(int(m["expY"]), int(m["expM"]), int(m["expD"]))
    obs = datetime.strptime(m["obsDate"] + m["obsTime"], "%Y%m%d%H%M%S")
    return SnapshotMeta(
        ticker=m["ticker"],
        spot_in_name=float(m["spot"]),
        expiration=exp,
        observed_dt=obs,
    )


def load_snapshot_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def iter_snapshot_files(snapshot_dir: Path, glob: str = "*.json"):
    for p in sorted(snapshot_dir.glob(glob)):
        if p.is_file():
            meta = parse_snapshot_filename(p.name)
            if meta is None:
                continue
            yield p, meta
