from __future__ import annotations

import argparse
import time
from pathlib import Path

import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class Handler(FileSystemEventHandler):
    def __init__(self, on_new_file):
        super().__init__()
        self.on_new_file = on_new_file

    def on_created(self, event):
        if event.is_directory:
            return
        self.on_new_file(Path(event.src_path))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    snap_dir = Path(cfg["snapshot_dir"]).expanduser()

    def on_new(p: Path):
        if p.suffix.lower() != ".json":
            return
        print(f"new snapshot: {p.name}")
        # MVP: just log. Next: append features row + re-score model + update plan.

    obs = Observer()
    obs.schedule(Handler(on_new), str(snap_dir), recursive=False)
    obs.start()
    print(f"watching {snap_dir} ...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        obs.stop()
        obs.join()


if __name__ == "__main__":
    main()
