"""LangGraph ReAct agent for import resolution with tool access.

This agent has access to:
- search_pypi: Check if a package exists on PyPI and get its metadata
- search_seed_data: Look up import→package mappings from seed TSV files
- reverse_lookup: Find which packages provide a given import name

The agent can reason iteratively, using tools to verify its guesses
before committing to a final answer.
"""

from __future__ import annotations

import json
import logging
from typing import Annotated, TypedDict

from pydantic import BaseModel, Field

from ..models import PackageMapping, ResolutionRequest, ResolutionResponse
from ..pypi_checker import package_exists_on_pypi
from ..reverse_index import lookup as reverse_lookup, fuzzy_lookup, load as load_reverse_index
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
            return json.dumps({
                "exists": True,
                "package": package_name,
                "summary": info.get("summary", "")[:200],
                "latest_version": info.get("version", ""),
                "requires_python": info.get("requires_python", ""),
            })
    except Exception as e:
        return json.dumps({"error": str(e), "package": package_name})
    return json.dumps({"exists": False, "package": package_name})


def search_seed_data(import_name: str) -> str:
    """Look up known import→package mappings from APDR seed data."""
    load_reverse_index()
    exact = reverse_lookup(import_name)
    fuzzy = fuzzy_lookup(import_name, limit=5)
    return json.dumps({
        "import_name": import_name,
        "exact_matches": exact,
        "fuzzy_matches": [f for f in fuzzy if f not in exact],
    })


def reverse_top_level(import_name: str) -> str:
    """Find packages that ship a given top-level import name."""
    load_reverse_index()
    packages = reverse_lookup(import_name)
    return json.dumps({
        "import_name": import_name,
        "packages": packages[:10],
    })


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

TOOL_DESCRIPTIONS = """
You have access to these tools:

1. search_pypi(package_name) - Check if a package exists on PyPI and get its summary
2. search_seed_data(import_name) - Look up known import→package mappings from local data
3. reverse_top_level(import_name) - Find packages that provide a given import

To use a tool, respond with:
TOOL_CALL: tool_name(argument)

After getting a tool result, continue reasoning. When ready, give your final answer as:
FINAL_ANSWER: {"mappings": [{"import_name": "...", "package_name": "..."}]}
"""

TOOLS = {
    "search_pypi": search_pypi,
    "search_seed_data": search_seed_data,
    "reverse_top_level": reverse_top_level,
}


def _try_langgraph_agent(req: ResolutionRequest) -> ResolutionResponse | None:
    """Try to use the full LangGraph ReAct agent. Returns None if langgraph not available."""
    try:
        from langgraph.prebuilt import create_react_agent
        from langchain_core.tools import tool as langchain_tool
        from langchain_community.chat_models import ChatLiteLLM
    except ImportError:
        return None

    # Wrap tools for LangChain
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

    tools = [lc_search_pypi, lc_search_seed_data, lc_reverse_top_level]

    model_name = f"ollama_chat/{req.model}" if req.provider == "ollama" else req.model
    llm = ChatLiteLLM(
        model=model_name,
        api_base=req.base_url if req.provider == "ollama" else None,
        temperature=0.0,
        max_tokens=1024,
    )

    agent = create_react_agent(llm, tools)

    system_msg = (
        f"{prompts.RESOLUTION_SYSTEM}\n\n"
        f"Target Python version: {req.python_version}\n"
    )
    context_str = "\n".join(req.context) if req.context else ""
    user_msg = (
        f"Resolve these Python imports to PyPI package names.\n"
        f"Context: {context_str}\n"
        f"Imports: {', '.join(req.imports)}\n\n"
        f"For each import, use the tools to verify your answer before responding. "
        f"First search_seed_data for known mappings, then search_pypi to verify existence."
    )

    try:
        result = agent.invoke({
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ]
        })

        # Extract the final message
        messages = result.get("messages", [])
        if not messages:
            return None

        final_msg = messages[-1]
        content = ""
        if hasattr(final_msg, "content"):
            content = final_msg.content
        elif isinstance(final_msg, dict):
            content = final_msg.get("content", "")

        # Try to parse JSON from the response
        mappings = _extract_mappings_from_text(content, req.imports)
        if mappings:
            return ResolutionResponse(
                mappings=mappings,
                notes=["Resolved via LangGraph ReAct agent"],
                prompts_issued=len(messages) // 2,
            )
    except Exception as e:
        logger.warning("LangGraph agent failed: %s", e)

    return None


def _manual_react_loop(req: ResolutionRequest) -> ResolutionResponse:
    """Manual ReAct loop without LangGraph dependency."""
    from ..client import LlmClient

    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        return ResolutionResponse(error="LLM provider not available")

    load_reverse_index()

    context_str = "\n".join(req.context) if req.context else "none"
    user_msg = (
        f"Target Python version: {req.python_version}\n"
        f"Context:\n{context_str}\n\n"
        f"Resolve these Python imports to PyPI packages: {', '.join(req.imports)}\n\n"
        f"{TOOL_DESCRIPTIONS}"
    )

    system_msg = prompts.RESOLUTION_SYSTEM
    conversation = user_msg
    prompts_issued = 0
    max_steps = 6  # Max tool-use iterations

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

        # Check for final answer
        if "FINAL_ANSWER:" in response:
            json_part = response.split("FINAL_ANSWER:", 1)[1].strip()
            mappings = _extract_mappings_from_text(json_part, req.imports)
            if mappings:
                return ResolutionResponse(
                    mappings=mappings,
                    notes=[f"ReAct agent resolved in {step + 1} steps"],
                    prompts_issued=prompts_issued,
                )

        # Check for tool call
        if "TOOL_CALL:" in response:
            tool_line = response.split("TOOL_CALL:", 1)[1].strip().split("\n")[0]
            tool_result = _execute_tool_call(tool_line)
            # Append to conversation
            conversation += f"\n\nAssistant: {response}\n\nTool result: {tool_result}\n\nContinue reasoning."
        else:
            # No tool call and no final answer — try to extract mappings from raw text
            mappings = _extract_mappings_from_text(response, req.imports)
            if mappings:
                return ResolutionResponse(
                    mappings=mappings,
                    notes=[f"ReAct agent resolved in {step + 1} steps (implicit)"],
                    prompts_issued=prompts_issued,
                )
            break

    # Fallback: identity mappings
    return ResolutionResponse(
        mappings=[
            PackageMapping(import_name=imp, package_name=imp)
            for imp in req.imports
        ],
        notes=["ReAct agent exhausted steps, returning identity mappings"],
        prompts_issued=prompts_issued,
    )


def _execute_tool_call(tool_line: str) -> str:
    """Parse and execute a TOOL_CALL: tool_name(argument) string."""
    try:
        paren = tool_line.index("(")
        tool_name = tool_line[:paren].strip()
        arg = tool_line[paren + 1:].rstrip(")").strip().strip("'\"")
        if tool_name in TOOLS:
            return TOOLS[tool_name](arg)
        return json.dumps({"error": f"Unknown tool: {tool_name}"})
    except Exception as e:
        return json.dumps({"error": f"Failed to parse tool call: {e}"})


def _extract_mappings_from_text(
    text: str, imports: list[str],
) -> list[PackageMapping]:
    """Try to extract import→package mappings from LLM text output."""
    # Try JSON parsing
    try:
        # Find JSON object in text
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

    # Try parsing "import -> package" lines
    mappings = []
    for line in text.splitlines():
        line = line.strip().lstrip("-•* ")
        if " -> " in line or " → " in line:
            sep = " -> " if " -> " in line else " → "
            parts = line.split(sep, 1)
            if len(parts) == 2:
                imp = parts[0].strip().strip("`")
                pkg = parts[1].strip().strip("`").split()[0]  # Take first word
                if imp in imports:
                    mappings.append(PackageMapping(import_name=imp, package_name=pkg))

    return mappings


def handle(req: ResolutionRequest) -> ResolutionResponse:
    """Handle resolution via ReAct agent. Tries LangGraph first, falls back to manual."""
    # Try LangGraph agent
    result = _try_langgraph_agent(req)
    if result is not None:
        return result

    # Fall back to manual ReAct loop
    return _manual_react_loop(req)
