from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence
import os
import platform
import shutil
import subprocess


RUN_CONTRACT_VERSION = 1
LLM_VALIDATION_POLICY_DOCKER_FIRST = "docker-first"
LLM_VALIDATION_POLICY_ENV_FIRST = "env-first"
REQUIRED_RUN_CONTRACT_KEYS = (
    "run_contract_version",
    "tool",
    "model_name",
    "base_url",
    "validation_backend",
    "llm_validation_policy",
    "run_intent",
    "execution_mode",
    "cache_state",
    "host_architecture",
    "apdr_binary_architecture",
    "python_architecture",
    "llm_context_window",
    "inference_policy",
    "build_profile",
)


def normalize_machine_architecture(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"aarch64", "arm64e"}:
        return "arm64"
    if text in {"amd64", "x64", "x86-64"}:
        return "x86_64"
    return text or "unknown"


def normalize_run_intent(value: Any) -> str:
    text = str(value or "").strip().lower().replace("_", "-")
    return text or "baseline"


def normalize_cache_state(value: Any) -> str:
    text = str(value or "").strip().lower().replace("_", "-")
    if text in {"warm", "cold", "mixed", "unknown"}:
        return text
    return "unknown"


def normalize_build_profile(value: Any) -> str:
    text = str(value or "").strip().lower().replace("_", "-")
    return text or "standard"


def normalize_context_window(value: Any) -> str:
    text = str(value or "").strip()
    if text:
        return text
    return (
        str(os.environ.get("APDR_NUM_CTX") or "").strip()
        or str(os.environ.get("OLLAMA_CONTEXT_LENGTH") or "").strip()
        or "16384"
    )


def normalize_inference_policy(value: Any, temperature: float | None = None) -> str:
    text = str(value or "").strip()
    if text:
        return text
    if temperature is None:
        return "temperature=inherited"
    return f"temperature={temperature}"


def normalize_llm_validation_policy(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text == LLM_VALIDATION_POLICY_DOCKER_FIRST:
        return LLM_VALIDATION_POLICY_DOCKER_FIRST
    return LLM_VALIDATION_POLICY_ENV_FIRST


def determine_execution_mode(tool: str, validation_backend: str) -> str:
    resolved_tool = str(tool or "").strip().lower()
    resolved_backend = str(validation_backend or "").strip().lower()
    if resolved_tool == "pllm" or resolved_backend == "docker":
        return "docker-proof"
    if resolved_backend == "llm":
        return "llm-hybrid"
    return "env-fast"


def contract_from_sources(*sources: Any) -> dict[str, Any]:
    contract: dict[str, Any] = {}
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        nested = source.get("run_contract")
        if isinstance(nested, Mapping):
            contract.update({str(key): nested[key] for key in nested})
        camel = source.get("runContract")
        if isinstance(camel, Mapping):
            contract.update({str(key): camel[key] for key in camel})
        if all(key in source for key in REQUIRED_RUN_CONTRACT_KEYS):
            contract.update({key: source[key] for key in REQUIRED_RUN_CONTRACT_KEYS})
    return contract


def missing_required_keys(contract: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    for key in REQUIRED_RUN_CONTRACT_KEYS:
        text = str(contract.get(key, "")).strip()
        if not text:
            missing.append(key)
    return missing


def detect_python_architecture(runner_command: Sequence[str]) -> str:
    fallback = normalize_machine_architecture(platform.machine())
    if not runner_command:
        return fallback
    command = list(runner_command) + [
        "-c",
        "import platform, struct; print(f\"{platform.machine()}-{struct.calcsize('P') * 8}\")",
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return fallback
    if completed.returncode != 0:
        return fallback
    text = str(completed.stdout or "").strip()
    if not text:
        return fallback
    arch, _, bits = text.partition("-")
    normalized = normalize_machine_architecture(arch)
    if bits:
        return f"{normalized}-{bits}"
    return normalized


def _find_apdr_binary(repo_root: Path) -> Path | None:
    tool_root = repo_root / "tools" / "apdr"
    candidates = (
        tool_root / "target" / "release" / "apdr",
        tool_root / "target" / "debug" / "apdr",
        tool_root / "target" / "release" / "apdr.exe",
        tool_root / "target" / "debug" / "apdr.exe",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def detect_apdr_binary_architecture(repo_root: Path, host_architecture: str = "") -> str:
    fallback = normalize_machine_architecture(host_architecture or platform.machine())
    binary_path = _find_apdr_binary(repo_root)
    if binary_path is None:
        return fallback
    if shutil.which("file"):
        try:
            completed = subprocess.run(
                ["file", str(binary_path)],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            completed = None
        if completed and completed.returncode == 0:
            text = str(completed.stdout or "").lower()
            if "arm64" in text or "aarch64" in text:
                return "arm64"
            if "x86_64" in text or "x86-64" in text or "amd64" in text:
                return "x86_64"
    return fallback


def build_run_contract(
    *,
    repo_root: Path,
    tool: str,
    model_name: str,
    base_url: str,
    temperature: float | None,
    validation_backend: str,
    run_config: Mapping[str, Any],
    runner_command: Sequence[str],
    host_architecture: str | None = None,
    python_architecture: str | None = None,
    apdr_binary_architecture: str | None = None,
) -> dict[str, str]:
    host_arch = normalize_machine_architecture(host_architecture or platform.machine())
    return {
        "run_contract_version": str(RUN_CONTRACT_VERSION),
        "tool": str(tool or "").strip(),
        "model_name": str(model_name or "").strip() or "unknown",
        "base_url": str(base_url or "").strip(),
        "validation_backend": str(validation_backend or "").strip().lower() or "env",
        "llm_validation_policy": normalize_llm_validation_policy(
            run_config.get("llm_validation_policy")
        ),
        "run_intent": normalize_run_intent(run_config.get("run_intent")),
        "execution_mode": determine_execution_mode(tool, validation_backend),
        "cache_state": normalize_cache_state(run_config.get("cache_state")),
        "host_architecture": host_arch,
        "apdr_binary_architecture": normalize_machine_architecture(
            apdr_binary_architecture or detect_apdr_binary_architecture(repo_root, host_arch)
        ),
        "python_architecture": str(
            python_architecture or detect_python_architecture(runner_command)
        ).strip()
        or host_arch,
        "llm_context_window": normalize_context_window(run_config.get("llm_context_window")),
        "inference_policy": normalize_inference_policy(
            run_config.get("inference_policy"),
            temperature,
        ),
        "build_profile": normalize_build_profile(run_config.get("build_profile")),
    }
