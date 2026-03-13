from __future__ import annotations

import argparse
import json

from .server import run_server
from .state import AppState


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Web benchmark UI for the FSE AIWare tool repository.")
    parser.add_argument("--doctor", action="store_true", help="Run environment checks and print them as JSON.")
    parser.add_argument("--list-tools", action="store_true", help="Print discovered tools and exit.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind the web server to.")
    parser.add_argument("--port", type=int, default=4173, help="Port to bind the web server to.")
    parser.add_argument(
        "--api-only",
        action="store_true",
        help="Serve only the JSON API; useful when running the Vite frontend separately.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    state = AppState()

    if args.list_tools:
        print(json.dumps(state.discover_tools(), indent=2))
        return

    if args.doctor:
        tool = state.discover_tools()[0] if state.discover_tools() else ""
        base_url = state.load_model_config(tool).base_url if tool else ""
        print(json.dumps(state.doctor_checks(tool, base_url), indent=2))
        return

    run_server(host=args.host, port=args.port, api_only=args.api_only, state=state)


if __name__ == "__main__":
    main()
