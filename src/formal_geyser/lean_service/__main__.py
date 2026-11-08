"""python -m formal_geyser.lean_service COMMAND --project PATH"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["serve", "supervise", "start", "stop", "restart", "status", "telemetry",
                                            "compact"])
    parser.add_argument("--project", type=Path, default=Path.cwd())
    parser.add_argument("--hours", type=float, default=24.0)
    args = parser.parse_args(argv)
    root = args.project.resolve()
    if args.command == "serve":
        from .server import serve
        return serve(root)
    if args.command == "compact":
        from .server import compact_store
        return compact_store(root)
    if args.command == "supervise":
        from .server import supervise
        return supervise(root)
    from .control import control
    print(json.dumps(asyncio.run(control(root, args.command, hours=args.hours)), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
