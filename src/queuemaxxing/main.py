from __future__ import annotations

import argparse
import os

import uvicorn

from queuemaxxing.api import create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Queuemaxxing frankenstein queue server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--data",
        default=os.environ.get("QUEUEMAXXING_DATA", "./data"),
        help="Root directory for per-queue WAL files",
    )
    args = parser.parse_args()
    os.environ["QUEUEMAXXING_DATA"] = args.data
    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
