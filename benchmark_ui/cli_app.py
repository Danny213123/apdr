from __future__ import annotations

from pathlib import Path
from typing import Any
import math
import os
import textwrap
import time

from . import APP_NAME, APP_VERSION
from .service import BenchmarkService
from .state import AppState


class BenchmarkCliApp:
    PAGES = [
        ("home", "Home"),
        ("run", "Run View"),
        ("configure", "Configure"),
        ("loadouts", "Loadouts"),
        ("doctor", "Doctor"),
        ("saved", "Saved Runs"),
    ]

    LIVE_RUN_STATUSES = {"booting", "running", "stopping", "completed", "failed", "stopped"}

    def __init__(self, state: AppState | None = None) -> None:
        self.service = BenchmarkService(state)
        bootstrap = self.service.bootstrap()
        self.app = dict(bootstrap["app"])
        self.form_config = dict(bootstrap["defaultConfig"])
        self.model_configs = dict(bootstrap["modelConfigs"])
        self.loadouts = list(bootstrap["loadouts"])
        self.runs = list(bootstrap["runs"])
        self.doctor = dict(bootstrap["doctor"])
        self.current_run = dict(bootstrap["currentRun"])
        self.preview = self.service.preview(self.form_config)
        self.page = "home"
        self.should_quit = False
        self.help_open = False
        self.message = "Terminal benchmark dashboard ready."
        self.message_level = "info"
        self.last_poll = 0.0
        self.last_support_refresh = 0.0
        self.historical_run: dict[str, Any] | None = None
        self.selection = {
            "run": 0,
            "loadouts": 0,
            "doctor": 0,
            "saved": 0,
        }

    def run(self) -> None:
        try:
            import curses
        except ImportError:
            self._run_plain_cli()
            return
        try:
            curses.wrapper(self._run_curses)
        except curses.error:
            self._notify("`curses` could not initialize cleanly, so the CLI is running in line mode.", "warn")
            self._run_plain_cli()

    def _run_curses(self, stdscr: Any) -> None:
        import curses

        curses.curs_set(0)
        stdscr.timeout(200)
        stdscr.keypad(True)
        self._init_colors(curses)
        while not self.should_quit:
            self._poll()
            self._draw(stdscr, curses)
            key = stdscr.getch()
            if key == -1:
                continue
            try:
                self._handle_key(stdscr, curses, key)
            except Exception as exc:  # pragma: no cover - defensive UI safety
                self._notify(f"{type(exc).__name__}: {exc}", "error")

    def _run_plain_cli(self) -> None:
        page_names = {name: title for name, title in self.PAGES}
        self._notify("`curses` is unavailable, so the CLI is running in line mode.", "warn")
        while not self.should_quit:
            self._poll()
            effective_run = self._effective_run()
            print()
            print(f"{APP_NAME} {APP_VERSION} [{page_names[self.page]}]")
            print("=" * 72)
            print(self.message)
            print("-" * 72)
            if self.page == "home":
                self._print_home(effective_run)
            elif self.page == "run":
                self._print_run(effective_run)
            elif self.page == "configure":
                self._print_configure()
            elif self.page == "loadouts":
                self._print_loadouts()
            elif self.page == "doctor":
                self._print_doctor()
            else:
                self._print_saved_runs()
            print("-" * 72)
            print("Pages: home run configure loadouts doctor saved | Actions: start stop doctor fix")
            print("Other: refresh load resume apply save-loadout delete-loadout tool dataset loop range")
            print("       limit python rag verbose refresh-models model base temp quit")
            raw = input("> ").strip()
            if not raw:
                continue
            command, _, remainder = raw.partition(" ")
            argument = remainder.strip()
            self._handle_plain_command(command.lower(), argument)

    def _print_home(self, run: dict[str, Any]) -> None:
        print(f"Tool: {self.form_config.get('tool') or '--'}")
        print(f"Dataset: {self.form_config.get('dataset_tar') or '--'}")
        print(
            "Run: "
            f"{run.get('status', 'idle')} | {run.get('completed', 0)}/{run.get('total', 0)} | "
            f"pass={run.get('successes', 0)} fail={run.get('failures', 0)} skip={run.get('skipped', 0)}"
        )
        print(f"Model: {self.preview.get('resolvedModel') or '--'}")

    def _print_run(self, run: dict[str, Any]) -> None:
        print(run.get("title") or "Run View")
        print(run.get("statusText") or "--")
        print(run.get("progressBar") or "--")
        print(
            f"Elapsed {run.get('elapsedLabel')} | Pace {run.get('speed')} | ETA {run.get('eta')} | "
            f"Pass rate {run.get('passRate')}"
        )
        print("Recent activity:")
        for line in (run.get("recentActivity") or [])[-8:]:
            print(f"  - {line}")

    def _print_configure(self) -> None:
        tool = str(self.form_config.get("tool") or "")
        model_config = self.model_configs.get(tool, {})
        print(f"Tool: {tool or '--'}")
        print(f"Dataset: {self.form_config.get('dataset_tar') or '--'}")
        print(
            f"Loop={self.form_config.get('loop_count')} Range={self.form_config.get('search_range')} "
            f"RAG={self.form_config.get('rag')} Verbose={self.form_config.get('verbose')}"
        )
        print(f"Snippet limit: {self.form_config.get('snippet_limit') or '--'}")
        print(f"Python override: {self.form_config.get('python_command') or '--'}")
        print(f"Model: {self.form_config.get('model') or model_config.get('model') or '--'}")
        print(f"Base URL: {self.form_config.get('base_url') or model_config.get('base_url') or '--'}")
        print(f"Temp: {self.form_config.get('temperature') or model_config.get('temperature') or '--'}")

    def _print_loadouts(self) -> None:
        if not self.loadouts:
            print("No loadouts saved.")
            return
        for index, item in enumerate(self.loadouts, start=1):
            marker = ">" if index - 1 == self.selection["loadouts"] else " "
            print(f"{marker} {index}. {item.get('name') or item.get('slug')}")

    def _print_doctor(self) -> None:
        print(self.doctor.get("summary") or "--")
        for row in (self.doctor.get("results") or [])[:12]:
            print(f"{row.get('status', '--'):4} {row.get('label', '--')}: {row.get('detail', '--')}")
        if self.doctor.get("logs"):
            print("Logs:")
            for line in self.doctor["logs"][-5:]:
                print(f"  {line}")

    def _print_saved_runs(self) -> None:
        if not self.runs:
            print("No saved runs found.")
            return
        for index, item in enumerate(self.runs, start=1):
            marker = ">" if index - 1 == self.selection["saved"] else " "
            print(f"{marker} {index}. {item.get('label') or item.get('runId')}")

    def _handle_plain_command(self, command: str, argument: str) -> None:
        page_lookup = {name: name for name, _title in self.PAGES}
        if command in page_lookup:
            self.page = page_lookup[command]
            return
        if command in {"quit", "exit", "q"}:
            self.should_quit = True
            return
        if command == "refresh":
            self._refresh_supporting_data(force=True)
            self._notify("Refreshed loadouts, runs, models, and preview.", "info")
            return
        if command == "start":
            self._start_benchmark()
            return
        if command == "stop":
            self._stop_benchmark()
            return
        if command == "doctor":
            self._start_doctor()
            return
        if command == "fix":
            self._start_doctor_fix()
            return
        if command == "resume":
            self._resume_saved_run(self._index_arg(argument, self.runs, "saved"))
            return
        if command == "load":
            self._load_saved_run(self._index_arg(argument, self.runs, "saved"))
            return
        if command == "apply":
            self._apply_loadout(self._index_arg(argument, self.loadouts, "loadouts"))
            return
        if command == "save-loadout":
            name = argument or input("Loadout name: ").strip()
            if name:
                self._save_loadout(name)
            return
        if command == "delete-loadout":
            self._delete_loadout(self._index_arg(argument, self.loadouts, "loadouts"))
            return
        if command == "tool":
            if argument:
                self._set_tool(argument)
            return
        if command == "dataset":
            if argument:
                self.form_config["dataset_tar"] = argument
                self.preview = self.service.preview(self.form_config)
                self._notify("Updated dataset archive path.", "info")
            return
        if command == "loop":
            if argument:
                self._set_int_config("loop_count", argument, "Loop count")
            return
        if command == "range":
            if argument:
                self._set_int_config("search_range", argument, "Search range")
            return
        if command == "limit":
            self.form_config["snippet_limit"] = argument
            self.preview = self.service.preview(self.form_config)
            self._notify("Updated snippet limit.", "info")
            return
        if command == "python":
            self.form_config["python_command"] = argument
            self.preview = self.service.preview(self.form_config)
            self._notify("Updated Python command override.", "info")
            return
        if command == "rag":
            self.form_config["rag"] = self._toggle_or_set_bool(self.form_config.get("rag"), argument)
            self.preview = self.service.preview(self.form_config)
            self._notify(f"RAG {'enabled' if self.form_config['rag'] else 'disabled'}.", "info")
            return
        if command == "verbose":
            self.form_config["verbose"] = self._toggle_or_set_bool(self.form_config.get("verbose"), argument)
            self.preview = self.service.preview(self.form_config)
            self._notify(f"Verbose {'enabled' if self.form_config['verbose'] else 'disabled'}.", "info")
            return
        if command == "refresh-models":
            self._refresh_models()
            return
        if command == "model":
            value = argument or input("Model: ").strip()
            if value:
                self._save_model_config(model=value)
            return
        if command == "base":
            value = argument or input("Base URL: ").strip()
            if value:
                self._save_model_config(base_url=value)
            return
        if command == "temp":
            value = argument or input("Temperature: ").strip()
            if value:
                self._save_model_config(temperature=value)
            return
        self._notify(f"Unknown command: {command}", "warn")

    def _index_arg(self, argument: str, items: list[dict[str, Any]], key: str) -> int:
        if argument:
            try:
                return max(0, min(len(items) - 1, int(argument) - 1))
            except ValueError:
                pass
        return int(self.selection.get(key, 0))

    def _init_colors(self, curses: Any) -> None:
        if not curses.has_colors():
            return
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_CYAN, -1)
        curses.init_pair(2, curses.COLOR_GREEN, -1)
        curses.init_pair(3, curses.COLOR_YELLOW, -1)
        curses.init_pair(4, curses.COLOR_RED, -1)
        curses.init_pair(5, curses.COLOR_WHITE, -1)

    def _poll(self) -> None:
        now = time.time()
        if now - self.last_poll >= 0.25:
            snapshot = self.service.status()
            self.current_run = dict(snapshot["currentRun"])
            self.doctor = dict(snapshot["doctor"])
            self.last_poll = now
        if now - self.last_support_refresh >= 3.0:
            self._refresh_supporting_data()
            self.last_support_refresh = now
        self._clamp_selections()

    def _refresh_supporting_data(self, force: bool = False) -> None:
        if force or self.page in {"configure", "loadouts", "saved", "home"}:
            self.model_configs = self.service.model_configs()
            self.loadouts = self.service.loadouts()
            self.runs = self.service.runs()
            self.preview = self.service.preview(self.form_config)
            self._sync_form_model_defaults()

    def _sync_form_model_defaults(self) -> None:
        tool = str(self.form_config.get("tool") or "")
        if not tool:
            self.preview = self.service.preview(self.form_config)
            return
        tool_config = self.model_configs.get(tool, {})
        if not str(self.form_config.get("model") or "").strip():
            self.form_config["model"] = str(tool_config.get("model") or "")
        if not str(self.form_config.get("base_url") or "").strip():
            self.form_config["base_url"] = str(tool_config.get("base_url") or "")
        if self.form_config.get("temperature") in {None, ""}:
            self.form_config["temperature"] = tool_config.get("temperature")
        self.preview = self.service.preview(self.form_config)

    def _effective_run(self) -> dict[str, Any]:
        if self.current_run.get("status") in self.LIVE_RUN_STATUSES or self.current_run.get("runId"):
            return self.current_run
        if self.historical_run:
            return self.historical_run
        return self.current_run

    def _clamp_selections(self) -> None:
        self.selection["run"] = self._clamp(self.selection["run"], len(self._effective_run().get("completedCases", [])))
        self.selection["loadouts"] = self._clamp(self.selection["loadouts"], len(self.loadouts))
        self.selection["doctor"] = self._clamp(self.selection["doctor"], len(self.doctor.get("results", [])))
        self.selection["saved"] = self._clamp(self.selection["saved"], len(self.runs))

    def _clamp(self, index: int, total: int) -> int:
        if total <= 0:
            return 0
        return max(0, min(index, total - 1))

    def _handle_key(self, stdscr: Any, curses: Any, key: int) -> None:
        if key in {ord("q"), ord("Q")}:
            self.should_quit = True
            return
        if key == ord("?"):
            self.help_open = not self.help_open
            return
        if key == 9:
            self._next_page()
            return
        if key == curses.KEY_BTAB:
            self._previous_page()
            return
        if ord("1") <= key <= ord("6"):
            self.page = self.PAGES[key - ord("1")][0]
            return
        if key in {ord("R")}:
            self._refresh_supporting_data(force=True)
            self._notify("Refreshed terminal data from the benchmark service.", "info")
            return

        if self.page == "home":
            self._handle_home_key(key)
            return
        if self.page == "run":
            self._handle_run_key(curses, key)
            return
        if self.page == "configure":
            self._handle_configure_key(stdscr, curses, key)
            return
        if self.page == "loadouts":
            self._handle_loadouts_key(stdscr, curses, key)
            return
        if self.page == "doctor":
            self._handle_doctor_key(curses, key)
            return
        if self.page == "saved":
            self._handle_saved_key(curses, key)

    def _handle_home_key(self, key: int) -> None:
        if key in {ord("s"), ord("S")}:
            self._start_benchmark()
        elif key in {ord("x"), ord("X")}:
            self._stop_benchmark()
        elif key in {ord("d"), ord("D")}:
            self.page = "doctor"
        elif key in {ord("c"), ord("C")}:
            self.page = "configure"
        elif key in {ord("v"), ord("V")}:
            self.page = "run"
        elif key in {ord("o"), ord("O")}:
            self.page = "saved"

    def _handle_run_key(self, curses: Any, key: int) -> None:
        cases = self._effective_run().get("completedCases", [])
        if key in {ord("x"), ord("X")}:
            self._stop_benchmark()
        elif key in {ord("l"), ord("L")}:
            self.page = "saved"
        elif key in {ord("h"), ord("H")}:
            self.page = "home"
        elif key in {ord("j"), curses.KEY_DOWN}:
            self.selection["run"] = self._clamp(self.selection["run"] + 1, len(cases))
        elif key in {ord("k"), curses.KEY_UP}:
            self.selection["run"] = self._clamp(self.selection["run"] - 1, len(cases))

    def _handle_configure_key(self, stdscr: Any, curses: Any, key: int) -> None:
        if key in {ord("t"), ord("T")}:
            options = self.app.get("tools") or []
            index = options.index(self.form_config["tool"]) if self.form_config.get("tool") in options else 0
            picked = self._pick_from_list(stdscr, curses, "Choose tool", options, index)
            if picked is not None:
                self._set_tool(options[picked])
        elif key in {ord("d"), ord("D")}:
            value = self._prompt(stdscr, curses, "Dataset archive path", str(self.form_config.get("dataset_tar") or ""))
            if value is not None:
                self.form_config["dataset_tar"] = value
                self.preview = self.service.preview(self.form_config)
                self._notify("Updated dataset archive path.", "info")
        elif key in {ord("l"), ord("L")}:
            self._prompt_int_field(stdscr, curses, "loop_count", "Loop count")
        elif key in {ord("r"), ord("R")}:
            self._prompt_int_field(stdscr, curses, "search_range", "Search range")
        elif key in {ord("n"), ord("N")}:
            value = self._prompt(stdscr, curses, "Snippet limit (blank for all)", str(self.form_config.get("snippet_limit") or ""))
            if value is not None:
                self.form_config["snippet_limit"] = value
                self.preview = self.service.preview(self.form_config)
                self._notify("Updated snippet limit.", "info")
        elif key in {ord("p"), ord("P")}:
            value = self._prompt(
                stdscr,
                curses,
                "Python command override",
                str(self.form_config.get("python_command") or ""),
            )
            if value is not None:
                self.form_config["python_command"] = value
                self.preview = self.service.preview(self.form_config)
                self._notify("Updated Python command override.", "info")
        elif key in {ord("a"), ord("A")}:
            self.form_config["rag"] = not bool(self.form_config.get("rag"))
            self.preview = self.service.preview(self.form_config)
            self._notify(f"RAG {'enabled' if self.form_config['rag'] else 'disabled'}.", "info")
        elif key in {ord("v"), ord("V")}:
            self.form_config["verbose"] = not bool(self.form_config.get("verbose"))
            self.preview = self.service.preview(self.form_config)
            self._notify(f"Verbose {'enabled' if self.form_config['verbose'] else 'disabled'}.", "info")
        elif key in {ord("f"), ord("F")}:
            self._refresh_models()
        elif key in {ord("m"), ord("M")}:
            self._edit_model(stdscr, curses)
        elif key in {ord("b"), ord("B")}:
            value = self._prompt(stdscr, curses, "Ollama base URL", self._current_base_url())
            if value is not None:
                self._save_model_config(base_url=value)
        elif key in {ord("u"), ord("U")}:
            value = self._prompt(stdscr, curses, "Temperature", str(self._current_temperature()))
            if value is not None:
                self._save_model_config(temperature=value)
        elif key in {ord("s"), ord("S")}:
            self._start_benchmark()

    def _handle_loadouts_key(self, stdscr: Any, curses: Any, key: int) -> None:
        if key in {ord("j"), curses.KEY_DOWN}:
            self.selection["loadouts"] = self._clamp(self.selection["loadouts"] + 1, len(self.loadouts))
        elif key in {ord("k"), curses.KEY_UP}:
            self.selection["loadouts"] = self._clamp(self.selection["loadouts"] - 1, len(self.loadouts))
        elif key in {ord("a"), ord("A"), 10, 13}:
            self._apply_loadout(self.selection["loadouts"])
        elif key in {ord("s"), ord("S")}:
            name = self._prompt(stdscr, curses, "Loadout name", str(self.form_config.get("loadout_name") or ""))
            if name:
                self._save_loadout(name)
        elif key in {ord("d"), ord("D")}:
            self._delete_loadout(self.selection["loadouts"])
        elif key in {ord("r"), ord("R")}:
            self._refresh_supporting_data(force=True)
            self._notify("Reloaded loadouts from disk.", "info")

    def _handle_doctor_key(self, curses: Any, key: int) -> None:
        if key in {ord("j"), curses.KEY_DOWN}:
            self.selection["doctor"] = self._clamp(self.selection["doctor"] + 1, len(self.doctor.get("results", [])))
        elif key in {ord("k"), curses.KEY_UP}:
            self.selection["doctor"] = self._clamp(self.selection["doctor"] - 1, len(self.doctor.get("results", [])))
        elif key in {ord("r"), ord("R")}:
            self._start_doctor()
        elif key in {ord("f"), ord("F")}:
            self._start_doctor_fix()

    def _handle_saved_key(self, curses: Any, key: int) -> None:
        if key in {ord("j"), curses.KEY_DOWN}:
            self.selection["saved"] = self._clamp(self.selection["saved"] + 1, len(self.runs))
        elif key in {ord("k"), curses.KEY_UP}:
            self.selection["saved"] = self._clamp(self.selection["saved"] - 1, len(self.runs))
        elif key in {ord("l"), ord("L"), 10, 13}:
            self._load_saved_run(self.selection["saved"])
        elif key in {ord("u"), ord("U")}:
            self._resume_saved_run(self.selection["saved"])
        elif key in {ord("r"), ord("R")}:
            self._refresh_supporting_data(force=True)
            self._notify("Reloaded saved runs.", "info")

    def _prompt_int_field(self, stdscr: Any, curses: Any, field: str, label: str) -> None:
        value = self._prompt(stdscr, curses, label, str(self.form_config.get(field) or ""))
        if value is None:
            return
        self._set_int_config(field, value, label)

    def _set_int_config(self, field: str, value: str, label: str) -> None:
        try:
            self.form_config[field] = int(value)
        except ValueError:
            self._notify(f"{label} must be an integer.", "error")
            return
        self.preview = self.service.preview(self.form_config)
        self._notify(f"Updated {label.lower()}.", "info")

    def _toggle_or_set_bool(self, current: Any, argument: str) -> bool:
        text = str(argument or "").strip().lower()
        if text in {"on", "true", "1", "yes"}:
            return True
        if text in {"off", "false", "0", "no"}:
            return False
        return not bool(current)

    def _set_tool(self, tool: str) -> None:
        if tool not in set(self.app.get("tools") or []):
            self._notify(f"Unknown tool: {tool}", "error")
            return
        self.form_config["tool"] = tool
        self.form_config["loadout_name"] = ""
        tool_config = self.model_configs.get(tool) or self.service.model_configs().get(tool, {})
        self.form_config["model"] = str(tool_config.get("model") or "")
        self.form_config["base_url"] = str(tool_config.get("base_url") or "")
        self.form_config["temperature"] = tool_config.get("temperature")
        self.preview = self.service.preview(self.form_config)
        self._notify(f"Selected tool `{tool}`.", "info")

    def _refresh_models(self) -> None:
        tool = str(self.form_config.get("tool") or "")
        if not tool:
            self._notify("Choose a tool first.", "warn")
            return
        response = self.service.refresh_models(tool, self._current_base_url())
        self.model_configs = response["allConfigs"]
        tool_config = response.get("config") or {}
        if tool_config:
            self.form_config["model"] = str(tool_config.get("model") or self.form_config.get("model") or "")
            self.form_config["base_url"] = str(tool_config.get("base_url") or self.form_config.get("base_url") or "")
            self.form_config["temperature"] = tool_config.get("temperature", self.form_config.get("temperature"))
        self.preview = self.service.preview(self.form_config)
        if response.get("models"):
            self._notify(
                f"Discovered {len(response['models'])} Ollama models via {response.get('source') or 'unknown source'}.",
                "info",
            )
        else:
            self._notify(response.get("error") or "No models discovered.", "warn")

    def _edit_model(self, stdscr: Any, curses: Any) -> None:
        tool = str(self.form_config.get("tool") or "")
        if not tool:
            self._notify("Choose a tool first.", "warn")
            return
        models = list((self.model_configs.get(tool) or {}).get("cached_models") or [])
        current_model = self._current_model()
        if models:
            index = models.index(current_model) if current_model in models else 0
            picked = self._pick_from_list(stdscr, curses, "Choose model", models, index)
            if picked is None:
                return
            self._save_model_config(model=models[picked])
            return
        value = self._prompt(stdscr, curses, "Model name", current_model)
        if value is not None:
            self._save_model_config(model=value)

    def _save_model_config(
        self,
        model: str | None = None,
        base_url: str | None = None,
        temperature: str | float | None = None,
    ) -> None:
        tool = str(self.form_config.get("tool") or "")
        if not tool:
            self._notify("Choose a tool first.", "warn")
            return
        current = self.model_configs.get(tool, {})
        if temperature is not None:
            try:
                parsed_temperature = float(temperature)
            except ValueError:
                self._notify("Temperature must be numeric.", "error")
                return
        else:
            parsed_temperature = float(current.get("temperature") or self.form_config.get("temperature") or 0.7)
        payload = {
            "configs": [
                {
                    "tool": tool,
                    "model": model if model is not None else self._current_model(),
                    "base_url": base_url if base_url is not None else self._current_base_url(),
                    "temperature": parsed_temperature,
                }
            ]
        }
        response = self.service.save_model_configs(payload)
        self.model_configs = response["modelConfigs"]
        updated = self.model_configs.get(tool, {})
        self.form_config["model"] = str(updated.get("model") or self.form_config.get("model") or "")
        self.form_config["base_url"] = str(updated.get("base_url") or self.form_config.get("base_url") or "")
        self.form_config["temperature"] = updated.get("temperature", parsed_temperature)
        self.preview = self.service.preview(self.form_config)
        self._notify(f"Saved model configuration for `{tool}`.", "info")

    def _current_model(self) -> str:
        tool = str(self.form_config.get("tool") or "")
        config = self.model_configs.get(tool, {})
        return str(self.form_config.get("model") or config.get("model") or "")

    def _current_base_url(self) -> str:
        tool = str(self.form_config.get("tool") or "")
        config = self.model_configs.get(tool, {})
        return str(self.form_config.get("base_url") or config.get("base_url") or "")

    def _current_temperature(self) -> float:
        tool = str(self.form_config.get("tool") or "")
        config = self.model_configs.get(tool, {})
        return float(self.form_config.get("temperature") or config.get("temperature") or 0.7)

    def _save_loadout(self, name: str) -> None:
        response = self.service.save_loadout({"name": name, "config": self.form_config})
        self.loadouts = response["loadouts"]
        self.form_config["loadout_name"] = name
        self._notify(f"Saved loadout `{name}`.", "info")

    def _apply_loadout(self, index: int) -> None:
        if not self.loadouts:
            self._notify("There are no loadouts to apply.", "warn")
            return
        item = self.loadouts[index]
        payload = dict(item)
        preview = self.service.preview(payload)
        self.form_config = dict(preview["config"])
        self.form_config["loadout_name"] = str(item.get("name") or item.get("slug") or "")
        self._sync_form_model_defaults()
        self._notify(f"Applied loadout `{item.get('name') or item.get('slug')}`.", "info")

    def _delete_loadout(self, index: int) -> None:
        if not self.loadouts:
            self._notify("There are no loadouts to delete.", "warn")
            return
        item = self.loadouts[index]
        response = self.service.delete_loadout(str(item.get("slug") or ""))
        self.loadouts = response["loadouts"]
        self._notify(f"Deleted loadout `{item.get('name') or item.get('slug')}`.", "info")

    def _load_saved_run(self, index: int) -> None:
        if not self.runs:
            self._notify("No saved runs are available.", "warn")
            return
        selected = self.runs[index]
        response = self.service.load_run(str(selected["runId"]))
        self.historical_run = response["run"]
        self.form_config = dict(response["formConfig"])
        self._sync_form_model_defaults()
        self.page = "run"
        self._notify(f"Loaded saved run {selected['runId']}.", "info")

    def _resume_saved_run(self, index: int) -> None:
        if not self.runs:
            self._notify("No saved runs are available.", "warn")
            return
        selected = self.runs[index]
        try:
            response = self.service.resume_run(str(selected["runId"]))
        except Exception as exc:
            self._notify(str(exc), "error")
            return
        self.current_run = response["currentRun"]
        self.runs = response["runs"]
        self.historical_run = None
        self.page = "run"
        self._notify(f"Resuming saved run {selected['runId']}.", "info")

    def _start_doctor(self) -> None:
        response = self.service.start_doctor(
            {
                "tool": self.form_config.get("tool"),
                "python_command": self.form_config.get("python_command"),
            }
        )
        self.doctor = response["doctor"]
        self.page = "doctor"
        self._notify("Doctor started.", "info")

    def _start_doctor_fix(self) -> None:
        response = self.service.start_doctor_fix(
            {
                "tool": self.form_config.get("tool"),
                "python_command": self.form_config.get("python_command"),
            }
        )
        self.doctor = response["doctor"]
        self.page = "doctor"
        self._notify("Doctor auto-fix started.", "info")

    def _start_benchmark(self) -> None:
        try:
            response = self.service.start_benchmark(self.form_config)
        except Exception as exc:
            self._notify(str(exc), "error")
            return
        self.current_run = response["currentRun"]
        self.runs = response["runs"]
        self.historical_run = None
        self.page = "run"
        self._notify("Benchmark started.", "info")

    def _stop_benchmark(self) -> None:
        response = self.service.stop_benchmark()
        self.current_run = response["currentRun"]
        self.runs = response["runs"]
        self._notify("Stop signal sent to the benchmark worker.", "warn")

    def _next_page(self) -> None:
        index = next(i for i, item in enumerate(self.PAGES) if item[0] == self.page)
        self.page = self.PAGES[(index + 1) % len(self.PAGES)][0]

    def _previous_page(self) -> None:
        index = next(i for i, item in enumerate(self.PAGES) if item[0] == self.page)
        self.page = self.PAGES[(index - 1) % len(self.PAGES)][0]

    def _notify(self, message: str, level: str = "info") -> None:
        self.message = message
        self.message_level = level

    def _draw(self, stdscr: Any, curses: Any) -> None:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        if height < 24 or width < 90:
            self._draw_small_terminal(stdscr, curses, height, width)
            stdscr.refresh()
            return
        self._draw_header(stdscr, curses, width)
        body_top = 3
        body_height = max(0, height - 6)
        if self.page == "home":
            self._draw_home(stdscr, curses, body_top, body_height, width)
        elif self.page == "run":
            self._draw_run(stdscr, curses, body_top, body_height, width)
        elif self.page == "configure":
            self._draw_configure(stdscr, curses, body_top, body_height, width)
        elif self.page == "loadouts":
            self._draw_loadouts(stdscr, curses, body_top, body_height, width)
        elif self.page == "doctor":
            self._draw_doctor(stdscr, curses, body_top, body_height, width)
        else:
            self._draw_saved_runs(stdscr, curses, body_top, body_height, width)
        self._draw_footer(stdscr, curses, height, width)
        if self.help_open:
            self._draw_help_overlay(stdscr, curses, height, width)
        stdscr.refresh()

    def _draw_small_terminal(self, stdscr: Any, curses: Any, height: int, width: int) -> None:
        self._add_line(
            stdscr,
            0,
            0,
            f"{APP_NAME} needs at least 90x24 for the full terminal UI. Current size: {width}x{height}",
            self._color(curses, "warn"),
        )
        self._add_line(stdscr, 2, 0, "Resize the terminal or run the web UI with `python3 -m benchmark_ui`.", 0)
        self._add_line(stdscr, 4, 0, f"Status: {self.message}", 0)
        self._add_line(stdscr, height - 1, 0, "Press q to quit.", self._color(curses, "accent"))

    def _draw_header(self, stdscr: Any, curses: Any, width: int) -> None:
        title = f"{APP_NAME} {APP_VERSION} | Terminal Dashboard"
        self._add_line(stdscr, 0, 0, title, curses.A_BOLD | self._color(curses, "accent"))
        tab_parts = []
        for index, (name, label) in enumerate(self.PAGES, start=1):
            token = f"[{index}] {label}"
            tab_parts.append(token if name != self.page else f"<{token}>")
        self._add_line(stdscr, 1, 0, "  ".join(tab_parts), self._color(curses, "normal"))
        self._add_line(stdscr, 2, 0, "-" * max(0, width - 1), self._color(curses, "dim"))

    def _draw_footer(self, stdscr: Any, curses: Any, height: int, width: int) -> None:
        self._add_line(stdscr, height - 3, 0, "-" * max(0, width - 1), self._color(curses, "dim"))
        self._add_line(stdscr, height - 2, 0, self.message, self._color(curses, self.message_level))
        help_text = self._page_help()
        self._add_line(stdscr, height - 1, 0, help_text, self._color(curses, "dim"))

    def _draw_home(self, stdscr: Any, curses: Any, top: int, height: int, width: int) -> None:
        effective_run = self._effective_run()
        left_width = max(30, width // 2 - 2)
        right_x = left_width + 3
        self._draw_section_title(stdscr, top, 0, "Home")
        self._add_wrapped(
            stdscr,
            top + 1,
            0,
            width - 2,
            effective_run.get("statusText") or self.preview.get("resolvedModel") or "",
            self._color(curses, "dim"),
            max_lines=2,
        )
        lines = [
            f"Tool: {self.form_config.get('tool') or '--'}",
            f"Dataset: {self._shorten_path(self.form_config.get('dataset_tar') or '--', left_width - 8)}",
            f"Loop count: {self.form_config.get('loop_count')}    Search range: {self.form_config.get('search_range')}",
            f"RAG: {'on' if self.form_config.get('rag') else 'off'}    Verbose: {'on' if self.form_config.get('verbose') else 'off'}",
            f"Snippet limit: {self.form_config.get('snippet_limit') or 'all'}",
            f"Python override: {self.form_config.get('python_command') or '--'}",
            f"Resolved model: {self.preview.get('resolvedModel') or '--'}",
        ]
        y = top + 4
        for line in lines:
            self._add_line(stdscr, y, 0, line, 0)
            y += 1

        self._draw_section_title(stdscr, top, right_x, "System + Actions")
        info_lines = [
            f"Version: {self.app.get('versionDisplay') or APP_VERSION}",
            f"Repo: {self._shorten_path(self.app.get('repoRoot') or '--', width - right_x - 3)}",
            f"OS: {(self.app.get('systemInfo') or {}).get('os') or '--'}",
            f"CPU: {(self.app.get('systemInfo') or {}).get('cpu') or '--'}",
            f"GPU: {(self.app.get('systemInfo') or {}).get('gpu') or '--'}",
            f"Memory: {(self.app.get('systemInfo') or {}).get('memory') or '--'}",
            f"Tools: {', '.join(self.app.get('tools') or []) or '--'}",
            "Actions: s=start  x=stop  c=configure  d=doctor  v=run view  o=saved runs",
        ]
        y = top + 1
        for line in info_lines:
            self._add_line(stdscr, y, right_x, line, 0)
            y += 1

        self._draw_section_title(stdscr, top + 10, 0, "Active Run Snapshot")
        summary_lines = [
            f"Title: {effective_run.get('title') or '--'}",
            f"Run ID: {effective_run.get('runId') or 'standby'}",
            f"Progress: {effective_run.get('completed', 0)}/{effective_run.get('total', 0)}",
            (
                f"Pass {effective_run.get('successes', 0)}  "
                f"Fail {effective_run.get('failures', 0)}  "
                f"Skip {effective_run.get('skipped', 0)}"
            ),
            f"Elapsed: {effective_run.get('elapsedLabel')}    Pace: {effective_run.get('speed')}    ETA: {effective_run.get('eta')}",
            f"Active case: {effective_run.get('activeCase') or '--'}",
        ]
        y = top + 11
        for line in summary_lines:
            self._add_line(stdscr, y, 0, line, 0)
            y += 1

        self._draw_section_title(stdscr, top + 10, right_x, "Recent Activity")
        activity_lines = list((effective_run.get("recentActivity") or [])[-10:])
        if not activity_lines:
            activity_lines = ["No benchmark activity yet."]
        y = top + 11
        for line in activity_lines[: max(0, height - 13)]:
            self._add_line(stdscr, y, right_x, self._truncate(line, width - right_x - 2), 0)
            y += 1

    def _draw_run(self, stdscr: Any, curses: Any, top: int, height: int, width: int) -> None:
        run = self._effective_run()
        self._draw_section_title(stdscr, top, 0, run.get("title") or "Run View")
        self._add_wrapped(
            stdscr,
            top + 1,
            0,
            width - 2,
            run.get("subtitle") or run.get("statusText") or "",
            self._color(curses, "dim"),
            max_lines=2,
        )
        self._add_line(stdscr, top + 3, 0, run.get("statusText") or "--", 0)
        self._add_line(stdscr, top + 4, 0, run.get("progressBar") or "--", self._color(curses, "accent"))
        metrics = (
            f"Successes: {run.get('successes', 0)}  Failures: {run.get('failures', 0)}  "
            f"Skips: {run.get('skipped', 0)}  Elapsed: {run.get('elapsedLabel')}  "
            f"Pass rate: {run.get('passRate')}  Pace: {run.get('speed')}  ETA: {run.get('eta')}"
        )
        self._add_line(stdscr, top + 5, 0, metrics, self._color(curses, "normal"))

        info_fields = list(run.get("infoFields") or [])
        info_rows = max(4, min(8, math.ceil(len(info_fields) / 2)))
        left_col_width = max(28, width // 2 - 2)
        right_col_x = left_col_width + 3
        self._draw_section_title(stdscr, top + 7, 0, "Run Info")
        for row_index in range(info_rows):
            left_index = row_index
            right_index = row_index + info_rows
            y = top + 8 + row_index
            if left_index < len(info_fields):
                item = info_fields[left_index]
                self._add_line(
                    stdscr,
                    y,
                    0,
                    f"{item['label']}: {self._truncate(item['value'], left_col_width - len(item['label']) - 3)}",
                    0,
                )
            if right_index < len(info_fields):
                item = info_fields[right_index]
                self._add_line(
                    stdscr,
                    y,
                    right_col_x,
                    f"{item['label']}: {self._truncate(item['value'], width - right_col_x - 2)}",
                    0,
                )

        panel_top = top + 9 + info_rows
        panel_height = max(8, height - (panel_top - top) - 1)
        left_panel_width = max(34, width // 2 - 2)
        right_panel_x = left_panel_width + 3
        self._draw_section_title(stdscr, panel_top, 0, "Recent Activity")
        self._draw_section_title(stdscr, panel_top, right_panel_x, "Completed Cases")

        activity_lines = list((run.get("recentActivity") or [])[-(panel_height - 2) :])
        if not activity_lines:
            activity_lines = ["No activity yet."]
        for offset, line in enumerate(activity_lines[: panel_height - 1], start=1):
            self._add_line(stdscr, panel_top + offset, 0, self._truncate(line, left_panel_width - 1), 0)

        cases = list(run.get("completedCases") or [])
        selected_index = self.selection["run"]
        selected_case = cases[selected_index] if cases else None
        case_list_height = max(4, panel_height - 7)
        list_start, visible_cases = self._window_for_selection(cases, selected_index, case_list_height)
        header = "STAT  CASE ID              PY    SEC     RESULT"
        self._add_line(stdscr, panel_top + 1, right_panel_x, header, self._color(curses, "accent"))
        for row_offset, case in enumerate(visible_cases, start=2):
            absolute_index = list_start + row_offset - 2
            marker_attr = curses.A_REVERSE if absolute_index == selected_index else 0
            status_text = str(case.get("status") or "--").ljust(4)
            case_id = self._truncate(str(case.get("caseId") or "--"), 20).ljust(20)
            py_value = str(case.get("python") or "--").ljust(5)
            sec_value = str(case.get("seconds") or "--").ljust(7)
            result_value = self._truncate(str(case.get("result") or "--"), width - right_panel_x - 42)
            line = f"{status_text}  {case_id} {py_value} {sec_value} {result_value}"
            self._add_line(stdscr, panel_top + row_offset, right_panel_x, line, marker_attr)

        detail_top = panel_top + case_list_height + 2
        detail_lines = ["No completed case selected."]
        if selected_case:
            detail_lines = [
                f"Case: {selected_case.get('caseId') or '--'} | Status: {selected_case.get('status') or '--'} | Python: {selected_case.get('python') or '--'}",
                f"Result: {selected_case.get('result') or '--'}",
                f"Dependencies: {selected_case.get('dependencies') or '--'}",
            ]
            tail = list(selected_case.get("logTail") or [])
            if tail:
                detail_lines.append(f"Log: {self._truncate(tail[-1], width - right_panel_x - 4)}")
        self._draw_section_title(stdscr, detail_top - 1, right_panel_x, "Selected Case")
        for offset, line in enumerate(detail_lines[:4]):
            self._add_line(stdscr, detail_top + offset, right_panel_x, self._truncate(line, width - right_panel_x - 2), 0)

    def _draw_configure(self, stdscr: Any, curses: Any, top: int, height: int, width: int) -> None:
        tool = str(self.form_config.get("tool") or "")
        tool_config = self.model_configs.get(tool, {})
        self._draw_section_title(stdscr, top, 0, "Configure")
        self._add_line(stdscr, top + 1, 0, "Edit the active run configuration and per-tool model settings.", self._color(curses, "dim"))
        fields = [
            ("Tool", tool or "--", "t"),
            ("Dataset", self._shorten_path(str(self.form_config.get("dataset_tar") or "--"), width - 18), "d"),
            ("Loop count", str(self.form_config.get("loop_count") or "--"), "l"),
            ("Search range", str(self.form_config.get("search_range") or "--"), "r"),
            ("Snippet limit", str(self.form_config.get("snippet_limit") or "all"), "n"),
            ("Python override", str(self.form_config.get("python_command") or "--"), "p"),
            ("RAG", "enabled" if self.form_config.get("rag") else "disabled", "a"),
            ("Verbose", "enabled" if self.form_config.get("verbose") else "disabled", "v"),
            ("Model", self._current_model() or "--", "m"),
            ("Base URL", self._current_base_url() or "--", "b"),
            ("Temperature", str(self._current_temperature()), "u"),
        ]
        y = top + 3
        for label, value, action in fields:
            self._add_line(stdscr, y, 0, f"[{action}] {label:<16} {self._truncate(value, width - 22)}", 0)
            y += 1
        self._draw_section_title(stdscr, top + 3, width // 2, "Tool Model Catalog")
        catalog_lines = [
            f"Current tool: {tool or '--'}",
            f"Saved model: {tool_config.get('model') or '--'}",
            f"Saved base URL: {tool_config.get('base_url') or '--'}",
            f"Saved temp: {tool_config.get('temperature') or '--'}",
            f"Updated: {tool_config.get('updated_at') or '--'}",
            f"Cached models: {', '.join(tool_config.get('cached_models') or []) or '--'}",
            "[f] refresh models from Ollama",
            "[s] start benchmark with this configuration",
        ]
        y = top + 4
        for line in catalog_lines:
            self._add_wrapped(stdscr, y, width // 2, width - width // 2 - 2, line, 0, max_lines=2)
            y += 2 if len(line) > (width - width // 2 - 2) else 1

    def _draw_loadouts(self, stdscr: Any, curses: Any, top: int, height: int, width: int) -> None:
        self._draw_section_title(stdscr, top, 0, "Loadouts")
        self._add_line(stdscr, top + 1, 0, "[j/k] move  [a or Enter] apply  [s] save current  [d] delete  [r] refresh", self._color(curses, "dim"))
        left_width = max(30, width // 2 - 2)
        start_index, visible_loadouts = self._window_for_selection(self.loadouts, self.selection["loadouts"], height - 6)
        for idx, item in enumerate(visible_loadouts, start=0):
            absolute_index = start_index + idx
            marker_attr = curses.A_REVERSE if absolute_index == self.selection["loadouts"] else 0
            label = f"{item.get('name') or item.get('slug')} | {item.get('tool') or '--'}"
            self._add_line(stdscr, top + 3 + idx, 0, self._truncate(label, left_width - 1), marker_attr)

        selected = self.loadouts[self.selection["loadouts"]] if self.loadouts else None
        detail_x = left_width + 3
        self._draw_section_title(stdscr, top, detail_x, "Selected Loadout")
        if not selected:
            self._add_line(stdscr, top + 1, detail_x, "No loadouts saved yet.", 0)
            return
        detail_lines = [
            f"Name: {selected.get('name') or selected.get('slug')}",
            f"Tool: {selected.get('tool') or '--'}",
            f"Dataset: {self._shorten_path(str(selected.get('dataset_tar') or '--'), width - detail_x - 4)}",
            f"Loop count: {selected.get('loop_count') or '--'}",
            f"Search range: {selected.get('search_range') or '--'}",
            f"RAG: {selected.get('rag')}",
            f"Verbose: {selected.get('verbose')}",
            f"Snippet limit: {selected.get('snippet_limit') or 'all'}",
            f"Python override: {selected.get('python_command') or '--'}",
            f"Updated: {selected.get('updated_at') or '--'}",
        ]
        for offset, line in enumerate(detail_lines, start=1):
            self._add_line(stdscr, top + offset, detail_x, self._truncate(line, width - detail_x - 2), 0)

    def _draw_doctor(self, stdscr: Any, curses: Any, top: int, height: int, width: int) -> None:
        self._draw_section_title(stdscr, top, 0, "Doctor")
        summary = self.doctor.get("summary") or "Doctor has not been run yet."
        busy_text = "busy" if self.doctor.get("busy") else "idle"
        self._add_line(stdscr, top + 1, 0, f"{summary} ({busy_text})", self._color(curses, "dim"))
        self._add_line(stdscr, top + 2, 0, "[r] run doctor  [f] auto-fix  [j/k] move", self._color(curses, "dim"))
        left_width = max(42, width // 2)
        results = list(self.doctor.get("results") or [])
        visible_rows = max(6, height - 8)
        start_index, visible = self._window_for_selection(results, self.selection["doctor"], visible_rows)
        header = "STAT  CHECK                    DETAIL"
        self._add_line(stdscr, top + 4, 0, header, self._color(curses, "accent"))
        for offset, row in enumerate(visible, start=1):
            absolute_index = start_index + offset - 1
            marker_attr = curses.A_REVERSE if absolute_index == self.selection["doctor"] else 0
            line = f"{str(row.get('status') or '--')[:4]:4}  {str(row.get('label') or '--')[:24]:24} {self._truncate(str(row.get('detail') or '--'), left_width - 34)}"
            self._add_line(stdscr, top + 4 + offset, 0, line, marker_attr)
        log_x = left_width + 2
        self._draw_section_title(stdscr, top, log_x, "Doctor Log")
        logs = list(self.doctor.get("logs") or [])
        if not logs:
            logs = ["No automatic setup log entries yet."]
        for offset, line in enumerate(logs[-(height - 5) :], start=1):
            self._add_line(stdscr, top + offset, log_x, self._truncate(line, width - log_x - 2), 0)

    def _draw_saved_runs(self, stdscr: Any, curses: Any, top: int, height: int, width: int) -> None:
        self._draw_section_title(stdscr, top, 0, "Saved Runs")
        self._add_line(stdscr, top + 1, 0, "[j/k] move  [l or Enter] load  [u] resume  [r] refresh", self._color(curses, "dim"))
        left_width = max(44, width // 2)
        visible_rows = max(6, height - 6)
        start_index, visible = self._window_for_selection(self.runs, self.selection["saved"], visible_rows)
        for offset, item in enumerate(visible, start=0):
            absolute_index = start_index + offset
            marker_attr = curses.A_REVERSE if absolute_index == self.selection["saved"] else 0
            self._add_line(
                stdscr,
                top + 3 + offset,
                0,
                self._truncate(str(item.get("label") or item.get("runId") or "--"), left_width - 1),
                marker_attr,
            )

        detail_x = left_width + 3
        self._draw_section_title(stdscr, top, detail_x, "Selected Run")
        selected = self.runs[self.selection["saved"]] if self.runs else None
        if not selected:
            self._add_line(stdscr, top + 1, detail_x, "No saved runs found.", 0)
            return
        detail_lines = [
            f"Run ID: {selected.get('runId') or '--'}",
            f"Status: {selected.get('status') or '--'}",
            f"Tool: {selected.get('tool') or '--'}",
            f"Completed: {selected.get('completed') or 0}/{selected.get('total') or 0}",
            f"Pass: {selected.get('successes') or 0}  Fail: {selected.get('failures') or 0}  Skip: {selected.get('skipped') or 0}",
            f"Remaining: {selected.get('remaining') or 0}",
            f"Resumable: {'yes' if selected.get('resumable') else 'no'}",
            f"Started: {selected.get('startedAt') or '--'}",
            f"Finished: {selected.get('finishedAt') or '--'}",
            f"Run dir: {selected.get('runDir') or '--'}",
        ]
        for offset, line in enumerate(detail_lines, start=1):
            self._add_line(stdscr, top + offset, detail_x, self._truncate(line, width - detail_x - 2), 0)

    def _draw_help_overlay(self, stdscr: Any, curses: Any, height: int, width: int) -> None:
        lines = [
            "Global",
            "  1-6 switch pages | Tab / Shift-Tab cycle pages | q quit | ? toggle help | R refresh",
            "",
            "Home",
            "  s start benchmark | x stop benchmark | c configure | d doctor | v run view | o saved runs",
            "",
            "Run View",
            "  j/k or arrows move completed-case selection | x stop benchmark",
            "",
            "Configure",
            "  t tool | d dataset | l loop count | r search range | n snippet limit | p python override",
            "  a toggle rag | v toggle verbose | m model | b base url | u temperature | f refresh models | s start",
            "",
            "Loadouts / Doctor / Saved Runs",
            "  j/k move selection; see footer for page actions",
        ]
        box_width = min(width - 8, 96)
        box_height = min(height - 6, len(lines) + 4)
        start_y = max(1, (height - box_height) // 2)
        start_x = max(2, (width - box_width) // 2)
        win = curses.newwin(box_height, box_width, start_y, start_x)
        win.erase()
        win.box()
        win.addnstr(0, 2, " Help ", box_width - 4, curses.A_BOLD | self._color(curses, "accent"))
        for idx, line in enumerate(lines[: box_height - 2], start=1):
            win.addnstr(idx, 2, line, box_width - 4, 0)
        win.refresh()

    def _page_help(self) -> str:
        if self.page == "home":
            return "Home: s=start  x=stop  c=configure  d=doctor  v=run view  o=saved runs  q=quit"
        if self.page == "run":
            return "Run View: j/k move cases  x=stop benchmark  l=saved runs  h=home  q=quit"
        if self.page == "configure":
            return "Configure: t tool  d dataset  l loop  r range  n limit  p python  a rag  v verbose  m model  b base  u temp  f refresh  s start"
        if self.page == "loadouts":
            return "Loadouts: j/k move  a/apply  s save current  d delete  r refresh  q=quit"
        if self.page == "doctor":
            return "Doctor: r run  f auto-fix  j/k move  q=quit"
        return "Saved Runs: j/k move  l load  u resume  r refresh  q=quit"

    def _draw_section_title(self, stdscr: Any, y: int, x: int, title: str) -> None:
        self._add_line(stdscr, y, x, title, 0)

    def _add_wrapped(
        self,
        stdscr: Any,
        y: int,
        x: int,
        width: int,
        text: str,
        attr: int = 0,
        max_lines: int | None = None,
    ) -> int:
        lines = self._wrap_text(text, width)
        if max_lines is not None:
            lines = lines[:max_lines]
        for offset, line in enumerate(lines):
            self._add_line(stdscr, y + offset, x, line, attr)
        return len(lines)

    def _add_line(self, stdscr: Any, y: int, x: int, text: str, attr: int = 0) -> None:
        height, width = stdscr.getmaxyx()
        if y < 0 or y >= height or x >= width:
            return
        available = max(0, width - x - 1)
        if available <= 0:
            return
        stdscr.addnstr(y, x, str(text), available, attr)

    def _wrap_text(self, text: str, width: int) -> list[str]:
        width = max(8, width)
        lines: list[str] = []
        for block in str(text).splitlines() or [""]:
            wrapped = textwrap.wrap(block, width=width) or [""]
            lines.extend(wrapped)
        return lines

    def _truncate(self, text: str, width: int) -> str:
        if width <= 0:
            return ""
        if len(text) <= width:
            return text
        if width <= 3:
            return text[:width]
        return text[: width - 3] + "..."

    def _shorten_path(self, value: str, width: int) -> str:
        text = str(value)
        if len(text) <= width:
            return text
        parts = Path(text).parts
        if len(parts) <= 2:
            return self._truncate(text, width)
        shortened = os.path.join("...", *parts[-3:])
        return self._truncate(shortened, width)

    def _slice_for_selection(self, items: list[Any], selected: int, size: int) -> list[Any]:
        _start, visible = self._window_for_selection(items, selected, size)
        return visible

    def _window_for_selection(self, items: list[Any], selected: int, size: int) -> tuple[int, list[Any]]:
        if size <= 0:
            return 0, []
        if len(items) <= size:
            return 0, items
        offset = self._visible_selection_offset(items, selected, size)
        start = max(0, selected - offset)
        end = min(len(items), start + size)
        start = max(0, end - size)
        return start, items[start:end]

    def _visible_selection_offset(self, items: list[Any], selected: int, size: int) -> int:
        if len(items) <= size:
            return selected
        middle = max(0, size // 2)
        return min(selected, middle)

    def _prompt(self, stdscr: Any, curses: Any, label: str, current: str) -> str | None:
        height, width = stdscr.getmaxyx()
        prompt = f"{label} [{current}]: "
        stdscr.timeout(-1)
        curses.echo()
        curses.curs_set(1)
        try:
            stdscr.move(height - 2, 0)
            stdscr.clrtoeol()
            stdscr.addnstr(height - 2, 0, prompt, max(1, width - 1))
            stdscr.refresh()
            raw = stdscr.getstr(height - 2, min(len(prompt), width - 2), max(1, width - len(prompt) - 2))
        finally:
            curses.noecho()
            curses.curs_set(0)
            stdscr.timeout(200)
        try:
            text = raw.decode("utf-8").strip()
        except Exception:
            return None
        if not text:
            return current
        return text

    def _pick_from_list(
        self,
        stdscr: Any,
        curses: Any,
        title: str,
        options: list[str],
        current_index: int = 0,
    ) -> int | None:
        if not options:
            return None
        selected = self._clamp(current_index, len(options))
        height, width = stdscr.getmaxyx()
        box_width = min(width - 8, max(32, min(80, max(len(title) + 8, max(len(item) for item in options) + 6))))
        visible_rows = min(max(6, len(options)), height - 8)
        box_height = visible_rows + 4
        start_y = max(1, (height - box_height) // 2)
        start_x = max(2, (width - box_width) // 2)
        win = curses.newwin(box_height, box_width, start_y, start_x)
        win.keypad(True)
        while True:
            win.erase()
            win.box()
            win.addnstr(0, 2, f" {title} ", box_width - 4, curses.A_BOLD | self._color(curses, "accent"))
            start, visible = self._window_for_selection(options, selected, visible_rows)
            for idx, item in enumerate(visible, start=0):
                absolute_index = start + idx
                attr = curses.A_REVERSE if absolute_index == selected else 0
                win.addnstr(2 + idx, 2, self._truncate(item, box_width - 4), box_width - 4, attr)
            win.refresh()
            key = win.getch()
            if key in {27, ord("q"), ord("Q")}:
                return None
            if key in {10, 13}:
                return selected
            if key in {ord("j"), curses.KEY_DOWN}:
                selected = self._clamp(selected + 1, len(options))
            elif key in {ord("k"), curses.KEY_UP}:
                selected = self._clamp(selected - 1, len(options))

    def _color(self, curses: Any, name: str) -> int:
        mapping = {
            "accent": 1,
            "success": 2,
            "warn": 3,
            "error": 4,
            "normal": 5,
            "dim": 5,
            "info": 1,
        }
        pair = mapping.get(name, 5)
        return curses.color_pair(pair) if curses.has_colors() else 0


def run_cli_app(state: AppState | None = None) -> None:
    BenchmarkCliApp(state).run()
