"""Explicit tier3 agent seam for import resolution with shared tool contracts.

This module exposes one benchmarkable agent interface that can run:
- a manual ReAct loop
- a LangGraph `create_react_agent` loop
- a LangChain `create_agent` loop

All modes share the same tool contract and return inspectable abstain reasons
when they cannot verify a package mapping.
"""

from __future__ import annotations

import json
import logging
from typing import Callable

from ..client import LlmClient, build_optional_langchain_chat_model
from ..models import PackageMapping, ResolutionRequest, ResolutionResponse
from ..pypi_checker import package_exists_on_pypi
from ..reverse_index import fuzzy_lookup, load as load_reverse_index, lookup as reverse_lookup
from .. import prompts

logger = logging.getLogger("apdr_llm")


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

def search_pypi(package_name: str) -> str:
    """Check if a package exists on PyPI. Returns JSON with exists/metadata."""
    try:
        import requests

        resp = requests.get(
            f"https://pypi.org/pypi/{package_name}/json",
            timeout=10,
        )
        if resp.status_code == 404:
            return json.dumps({"exists": False, "package": package_name})
        if resp.ok:
            data = resp.json()
            info = data.get("info", {})
            return json.dumps(
                {
                    "exists": True,
                    "package": package_name,
                    "summary": info.get("summary", "")[:200],
                    "latest_version": info.get("version", ""),
                    "requires_python": info.get("requires_python", ""),
                }
            )
    except Exception as exc:
        return json.dumps({"error": str(exc), "package": package_name})
    return json.dumps({"exists": False, "package": package_name})


def search_seed_data(import_name: str) -> str:
    """Look up known import→package mappings from APDR seed data."""
    load_reverse_index()
    exact = reverse_lookup(import_name)
    fuzzy = fuzzy_lookup(import_name, limit=5)
    return json.dumps(
        {
            "import_name": import_name,
            "exact_matches": exact,
            "fuzzy_matches": [item for item in fuzzy if item not in exact],
        }
    )


def reverse_top_level(import_name: str) -> str:
    """Find packages that ship a given top-level import name."""
    load_reverse_index()
    packages = reverse_lookup(import_name)
    return json.dumps({"import_name": import_name, "packages": packages[:10]})


ToolFn = Callable[[str], str]

TOOL_REGISTRY: dict[str, tuple[ToolFn, str]] = {
    "search_pypi": (
        search_pypi,
        "Check whether a package exists on PyPI and inspect summary or Python constraints.",
    ),
    "search_seed_data": (
        search_seed_data,
        "Look up known import-to-package mappings from APDR reverse-index and seed data.",
    ),
    "reverse_top_level": (
        reverse_top_level,
        "Find packages that ship a given top-level import name.",
    ),
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _normalize_agent_mode(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"langchain", "langgraph", "manual", "auto"}:
        return text
    if text in {"react", "react-manual"}:
        return "manual"
    if text in {"", "direct", "disabled"}:
        return "manual"
    return "manual"


def _normalize_tool_profile(value: str) -> str:
    text = str(value or "").strip().lower().replace("_", "-")
    if text in {"verify-only", "pypi-only"}:
        return "verify-only"
    if text in {"reduced", "reduced-toolset", "seed-and-verify"}:
        return "reduced-toolset"
    return "full"


def _default_policy_label(req: ResolutionRequest, actual_agent_mode: str) -> str:
    requested = str(req.policy_label or "").strip()
    if requested:
        return requested
    return f"{actual_agent_mode}-{_normalize_tool_profile(req.tool_profile)}"


def _response_metadata(req: ResolutionRequest, actual_agent_mode: str) -> dict[str, str]:
    return {
        "agent_mode": actual_agent_mode,
        "tool_profile": _normalize_tool_profile(req.tool_profile),
        "retrieval_profile": str(req.retrieval_profile or "").strip() or "none",
        "policy_label": _default_policy_label(req, actual_agent_mode),
    }


def _abstain_response(
    req: ResolutionRequest,
    reason: str,
    *,
    agent_mode: str,
    prompts_issued: int = 0,
    notes: list[str] | None = None,
    failure_reason: str = "",
) -> ResolutionResponse:
    response_notes = list(notes or [])
    response_notes.append(reason)
    return ResolutionResponse(
        unresolved=list(req.imports),
        notes=response_notes,
        prompts_issued=prompts_issued,
        abstain_reason=reason,
        failure_reason=failure_reason,
        **_response_metadata(req, agent_mode),
    )


def _select_tool_names(tool_profile: str) -> list[str]:
    normalized = _normalize_tool_profile(tool_profile)
    if normalized == "verify-only":
        return ["search_pypi"]
    if normalized == "reduced-toolset":
        return ["search_seed_data", "search_pypi"]
    return ["search_seed_data", "reverse_top_level", "search_pypi"]


def _tool_descriptions(tool_names: list[str]) -> str:
    lines = ["You have access to these tools:"]
    for index, tool_name in enumerate(tool_names, start=1):
        _, description = TOOL_REGISTRY[tool_name]
        lines.append(f"{index}. {tool_name}(argument) - {description}")
    lines.extend(
        [
            "",
            "To use a tool, respond with:",
            "TOOL_CALL: tool_name(argument)",
            "",
            "After getting a tool result, continue reasoning. When ready, give your final answer as:",
            'FINAL_ANSWER: {"mappings": [{"import_name": "...", "package_name": "..."}]}',
        ]
    )
    return "\n".join(lines)


def _build_user_message(req: ResolutionRequest, tool_names: list[str]) -> str:
    context_lines = list(req.context)
    context_lines.append(f"Retrieval profile: {str(req.retrieval_profile or '').strip() or 'none'}")
    context_lines.append(f"Tool profile: {_normalize_tool_profile(req.tool_profile)}")
    context_lines.append(f"Policy label: {_default_policy_label(req, _normalize_agent_mode(req.agent_mode))}")
    context_str = "\n".join(context_lines) if context_lines else "none"
    return (
        f"Target Python version: {req.python_version}\n"
        f"Context:\n{context_str}\n\n"
        f"Resolve these Python imports to PyPI packages: {', '.join(req.imports)}\n\n"
        f"{_tool_descriptions(tool_names)}\n\n"
        f"Use tools to verify mappings before responding. Prefer search_seed_data or reverse_top_level first, "
        f"then confirm the package with search_pypi before finalizing an answer."
    )


def _langchain_tools(tool_names: list[str]) -> tuple[list[object] | None, str]:
    try:
        from langchain_core.tools import tool as langchain_tool
    except ImportError as exc:
        return None, f"Optional LangChain tools unavailable: {exc}"

    @langchain_tool
    def lc_search_pypi(package_name: str) -> str:
        """Check if a package exists on PyPI and get its metadata."""
        return search_pypi(package_name)

    @langchain_tool
    def lc_search_seed_data(import_name: str) -> str:
        """Look up known import-to-package mappings from local seed data."""
        return search_seed_data(import_name)

    @langchain_tool
    def lc_reverse_top_level(import_name: str) -> str:
        """Find packages that ship a given top-level import name."""
        return reverse_top_level(import_name)

    wrapped = {
        "search_pypi": lc_search_pypi,
        "search_seed_data": lc_search_seed_data,
        "reverse_top_level": lc_reverse_top_level,
    }
    return [wrapped[name] for name in tool_names], ""


def _extract_agent_text(result: object) -> str:
    if isinstance(result, dict):
        messages = result.get("messages")
        if isinstance(messages, list) and messages:
            final_msg = messages[-1]
            if hasattr(final_msg, "content"):
                return str(final_msg.content or "")
            if isinstance(final_msg, dict):
                return str(final_msg.get("content") or "")
        output = result.get("output")
        if isinstance(output, str):
            return output
    if hasattr(result, "content"):
        return str(getattr(result, "content") or "")
    return str(result or "")


def _verify_mappings(
    req: ResolutionRequest,
    mappings: list[PackageMapping],
) -> tuple[list[PackageMapping], list[str], list[str]]:
    verified: list[PackageMapping] = []
    unresolved = set(req.imports)
    rejected: list[str] = []

    for mapping in mappings:
        import_name = str(mapping.import_name or "").strip()
        package_name = str(mapping.package_name or "").strip()
        if not import_name or not package_name or import_name not in req.imports:
            continue
        if package_exists_on_pypi(package_name):
            verified.append(PackageMapping(import_name=import_name, package_name=package_name))
            unresolved.discard(import_name)
        else:
            rejected.append(f"{import_name}->{package_name}")

    return verified, sorted(unresolved), rejected


def _verified_response(
    req: ResolutionRequest,
    mappings: list[PackageMapping],
    *,
    agent_mode: str,
    prompts_issued: int,
    notes: list[str] | None = None,
) -> ResolutionResponse:
    verified, unresolved, rejected = _verify_mappings(req, mappings)
    response_notes = list(notes or [])
    if rejected:
        response_notes.append(
            "Rejected unverified package mappings: " + ", ".join(rejected[:5])
        )
    if not verified:
        return _abstain_response(
            req,
            "Agent could not verify any proposed package mapping.",
            agent_mode=agent_mode,
            prompts_issued=prompts_issued,
            notes=response_notes,
        )

    abstain_reason = ""
    if unresolved:
        abstain_reason = "Agent abstained on imports without a verified mapping."
        response_notes.append(f"Agent abstained on: {', '.join(unresolved[:5])}")

    return ResolutionResponse(
        mappings=verified,
        unresolved=unresolved,
        notes=response_notes,
        prompts_issued=prompts_issued,
        abstain_reason=abstain_reason,
        **_response_metadata(req, agent_mode),
    )


def _execute_tool_call(tool_names: list[str], tool_line: str) -> str:
    """Parse and execute a TOOL_CALL: tool_name(argument) string."""
    try:
        paren = tool_line.index("(")
        tool_name = tool_line[:paren].strip()
        arg = tool_line[paren + 1 :].rstrip(")").strip().strip("'\"")
        if tool_name not in tool_names:
            return json.dumps({"error": f"Tool not enabled for this profile: {tool_name}"})
        tool_fn, _ = TOOL_REGISTRY[tool_name]
        return tool_fn(arg)
    except Exception as exc:
        return json.dumps({"error": f"Failed to parse tool call: {exc}"})


def _extract_mappings_from_text(text: str, imports: list[str]) -> list[PackageMapping]:
    """Try to extract import→package mappings from LLM text output."""
    try:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            data = json.loads(text[start:end])
            mappings_data = data.get("mappings", [])
            if mappings_data:
                return [
                    PackageMapping(
                        import_name=m.get("import_name", ""),
                        package_name=m.get("package_name", ""),
                    )
                    for m in mappings_data
                    if m.get("import_name") and m.get("package_name")
                ]
    except (json.JSONDecodeError, ValueError):
        pass

    mappings: list[PackageMapping] = []
    for line in text.splitlines():
        cleaned = line.strip().lstrip("-•* ")
        if " -> " in cleaned or " → " in cleaned:
            separator = " -> " if " -> " in cleaned else " → "
            left, right = cleaned.split(separator, 1)
            import_name = left.strip().strip("`")
            package_name = right.strip().strip("`").split()[0]
            if import_name in imports and package_name:
                mappings.append(
                    PackageMapping(import_name=import_name, package_name=package_name)
                )

    return mappings


# ---------------------------------------------------------------------------
# Agent implementations
# ---------------------------------------------------------------------------

def _try_langgraph_agent(
    req: ResolutionRequest,
    tool_names: list[str],
) -> tuple[ResolutionResponse | None, str]:
    try:
        from langgraph.prebuilt import create_react_agent
    except ImportError as exc:
        return None, f"LangGraph agent unavailable: {exc}"

    tools, reason = _langchain_tools(tool_names)
    if tools is None:
        return None, reason

    llm, llm_reason = build_optional_langchain_chat_model(
        req.provider,
        req.model,
        req.base_url,
        temperature=0.0,
        max_tokens=1024,
    )
    if llm is None:
        return None, llm_reason

    agent = create_react_agent(llm, tools)
    user_msg = _build_user_message(req, tool_names)
    system_msg = prompts.RESOLUTION_SYSTEM

    try:
        result = agent.invoke(
            {
                "messages": [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ]
            }
        )
    except Exception as exc:
        logger.warning("LangGraph agent failed: %s", exc)
        return None, f"LangGraph agent failed: {exc}"

    content = _extract_agent_text(result)
    mappings = _extract_mappings_from_text(content, req.imports)
    if not mappings:
        return None, "LangGraph agent returned no parseable mappings."
    return (
        _verified_response(
            req,
            mappings,
            agent_mode="langgraph",
            prompts_issued=1,
            notes=["Resolved via LangGraph create_react_agent"],
        ),
        "",
    )


def _try_langchain_agent(
    req: ResolutionRequest,
    tool_names: list[str],
) -> tuple[ResolutionResponse | None, str]:
    try:
        from langchain.agents import create_agent
    except ImportError as exc:
        return None, f"LangChain agent unavailable: {exc}"

    tools, reason = _langchain_tools(tool_names)
    if tools is None:
        return None, reason

    llm, llm_reason = build_optional_langchain_chat_model(
        req.provider,
        req.model,
        req.base_url,
        temperature=0.0,
        max_tokens=1024,
    )
    if llm is None:
        return None, llm_reason

    user_msg = _build_user_message(req, tool_names)
    system_msg = prompts.RESOLUTION_SYSTEM

    try:
        try:
            agent = create_agent(model=llm, tools=tools, system_prompt=system_msg)
        except TypeError:
            agent = create_agent(model=llm, tools=tools)
        result = agent.invoke({"messages": [{"role": "user", "content": user_msg}]})
    except Exception as exc:
        logger.warning("LangChain agent failed: %s", exc)
        return None, f"LangChain agent failed: {exc}"

    content = _extract_agent_text(result)
    mappings = _extract_mappings_from_text(content, req.imports)
    if not mappings:
        return None, "LangChain agent returned no parseable mappings."
    return (
        _verified_response(
            req,
            mappings,
            agent_mode="langchain",
            prompts_issued=1,
            notes=["Resolved via LangChain create_agent"],
        ),
        "",
    )


def _manual_react_loop(
    req: ResolutionRequest,
    tool_names: list[str],
    *,
    preface_notes: list[str] | None = None,
) -> ResolutionResponse:
    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        return ResolutionResponse(
            error="LLM provider not available",
            failure_reason="LLM provider not available",
            **_response_metadata(req, "manual"),
        )

    load_reverse_index()

    conversation = _build_user_message(req, tool_names)
    system_msg = prompts.RESOLUTION_SYSTEM
    prompts_issued = 0
    notes = list(preface_notes or [])
    max_steps = 6

    for step in range(max_steps):
        response = client.complete(
            system_prompt=system_msg,
            user_prompt=conversation,
            temperature=0.0,
            max_tokens=1024,
        )
        prompts_issued += 1

        if response is None:
            break

        if "FINAL_ANSWER:" in response:
            json_part = response.split("FINAL_ANSWER:", 1)[1].strip()
            mappings = _extract_mappings_from_text(json_part, req.imports)
            if mappings:
                return _verified_response(
                    req,
                    mappings,
                    agent_mode="manual",
                    prompts_issued=prompts_issued,
                    notes=notes + [f"Manual ReAct agent resolved in {step + 1} steps"],
                )

        if "TOOL_CALL:" in response:
            tool_line = response.split("TOOL_CALL:", 1)[1].strip().split("\n")[0]
            tool_result = _execute_tool_call(tool_names, tool_line)
            conversation += (
                f"\n\nAssistant: {response}\n\n"
                f"Tool result: {tool_result}\n\n"
                f"Continue reasoning and verify before finalizing."
            )
            continue

        mappings = _extract_mappings_from_text(response, req.imports)
        if mappings:
            return _verified_response(
                req,
                mappings,
                agent_mode="manual",
                prompts_issued=prompts_issued,
                notes=notes + [f"Manual ReAct agent resolved in {step + 1} steps (implicit)"],
            )
        break

    return _abstain_response(
        req,
        "Manual ReAct agent exhausted steps without a verified mapping.",
        agent_mode="manual",
        prompts_issued=prompts_issued,
        notes=notes,
    )


def handle(req: ResolutionRequest) -> ResolutionResponse:
    """Handle resolution via one explicit agent seam."""
    requested_mode = _normalize_agent_mode(req.agent_mode)
    tool_names = _select_tool_names(req.tool_profile)

    if requested_mode == "langgraph":
        result, reason = _try_langgraph_agent(req, tool_names)
        if result is not None:
            return result
        return _manual_react_loop(
            req,
            tool_names,
            preface_notes=[f"LangGraph unavailable; fell back to manual agent: {reason}"],
        )

    if requested_mode == "langchain":
        result, reason = _try_langchain_agent(req, tool_names)
        if result is not None:
            return result
        return _manual_react_loop(
            req,
            tool_names,
            preface_notes=[f"LangChain unavailable; fell back to manual agent: {reason}"],
        )

    if requested_mode == "auto":
        for mode_name, runner in (
            ("langchain", _try_langchain_agent),
            ("langgraph", _try_langgraph_agent),
        ):
            result, reason = runner(req, tool_names)
            if result is not None:
                return result
            logger.info("Tier3 auto agent skipped %s path: %s", mode_name, reason)
        return _manual_react_loop(
            req,
            tool_names,
            preface_notes=["Auto agent fell back to manual after optional agent runtimes were unavailable."],
        )

    return _manual_react_loop(req, tool_names)
