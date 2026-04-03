"""Docker authoring action derived from the authored case plan."""

from __future__ import annotations

import json

from ..client import LlmClient
from ..models import (
    AuthoredCasePlan,
    AuthoredDockerPlan,
    ResolutionRequest,
    ResolutionResponse,
)


DOCKER_PLAN_SYSTEM = """\
You convert an authored APDR case plan into Docker validation intent.

Rules:
1. Consume the provided authored case plan as the source of truth. Do not re-parse the snippet.
2. Prefer the smallest reproducible Docker plan that can validate the case.
3. Keep the output structured. Do not emit a raw Dockerfile.
4. Include only system packages that are plausibly required for build/runtime.
5. Keep working_directory as /app unless there is a strong reason not to.
6. Default the command to running the generated smoke test.
"""


def _default_base_image(python_version: str) -> str:
    return f"python:{python_version}-slim"


def _normalize_docker_plan(
    plan: AuthoredDockerPlan,
    authored_plan: AuthoredCasePlan,
    python_version: str,
) -> AuthoredDockerPlan:
    if not plan.plan_version.strip():
        plan.plan_version = "1"
    if not plan.base_image.strip():
        plan.base_image = _default_base_image(python_version)
    if not plan.working_directory.strip():
        plan.working_directory = "/app"
    if not plan.command:
        plan.command = ["python", "/app/smoke_test.py"]
    if not plan.smoke_strategy.import_targets and authored_plan.smoke_strategy.import_targets:
        plan.smoke_strategy = authored_plan.smoke_strategy.model_copy(deep=True)
    plan.system_packages = sorted({item.strip() for item in plan.system_packages if item.strip()})
    plan.environment_variables = [
        item.strip() for item in plan.environment_variables if item.strip()
    ]
    if not plan.rationale.strip():
        plan.rationale = "Docker validation derived from the authored case plan."
    if not plan.section_confidence:
        plan.section_confidence = {
            "base_image": 0.9,
            "system_packages": 0.75,
            "smoke_strategy": 0.9,
        }
    if not plan.authorship.strip():
        plan.authorship = "llm-authored"
    plan.deterministic_fallback_sections = [
        item.strip()
        for item in plan.deterministic_fallback_sections
        if item.strip()
    ]
    return plan


def _deterministic_docker_plan(
    authored_plan: AuthoredCasePlan,
    python_version: str,
) -> AuthoredDockerPlan:
    return AuthoredDockerPlan(
        plan_version="1",
        base_image=_default_base_image(python_version),
        system_packages=sorted(
            {
                item.strip()
                for item in authored_plan.system_dependency_hints
                if item.strip()
            }
        ),
        environment_variables=[],
        working_directory="/app",
        command=["python", "/app/smoke_test.py"],
        smoke_strategy=authored_plan.smoke_strategy.model_copy(deep=True),
        rationale="Docker plan synthesized deterministically from the authored case plan.",
        section_confidence={
            "base_image": 1.0,
            "system_packages": 1.0,
            "smoke_strategy": 1.0,
        },
        authorship="deterministic-fallback",
        deterministic_fallback_sections=["phase26-case-plan"],
    )


def _build_user_prompt(authored_plan: AuthoredCasePlan, python_version: str) -> str:
    authored_plan_json = json.dumps(
        authored_plan.model_dump(mode="json"),
        indent=2,
        sort_keys=True,
    )
    return (
        "Author a structured Docker validation plan from this APDR case plan.\n\n"
        f"python_version: {python_version}\n\n"
        "Return JSON fields for base_image, system_packages, environment_variables, "
        "working_directory, command, smoke_strategy, rationale, section_confidence, "
        "authorship, and deterministic_fallback_sections.\n\n"
        "Authored case plan:\n"
        f"{authored_plan_json}"
    )


def handle(req: ResolutionRequest) -> ResolutionResponse:
    if req.authored_plan is None:
        return ResolutionResponse(
            docker_plan_status="not-requested",
            notes=["No authored case plan available for Docker authoring."],
            prompts_issued=0,
        )

    authored_plan = req.authored_plan
    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        docker_plan = _deterministic_docker_plan(authored_plan, req.python_version)
        return ResolutionResponse(
            docker_plan=docker_plan,
            docker_plan_status="deterministic-fallback",
            notes=["Docker authoring fell back deterministically because the LLM provider was unavailable."],
            prompts_issued=0,
            failure_reason="LLM provider not available",
        )

    result = client.complete_json(
        system_prompt=DOCKER_PLAN_SYSTEM,
        user_prompt=_build_user_prompt(authored_plan, req.python_version),
        response_model=AuthoredDockerPlan,
        max_tokens=1024,
        num_ctx=8192,
    )
    if result is None:
        diagnostics_raw = client.last_failure_reason()
        diagnostics = diagnostics_raw.strip() if isinstance(diagnostics_raw, str) else ""
        docker_plan = _deterministic_docker_plan(authored_plan, req.python_version)
        notes = ["Docker authoring fell back deterministically after unusable LLM output."]
        if diagnostics:
            notes.append(f"LLM diagnostics: {diagnostics}")
        return ResolutionResponse(
            docker_plan=docker_plan,
            docker_plan_status="deterministic-fallback",
            notes=notes,
            prompts_issued=1,
            failure_reason=diagnostics,
        )

    docker_plan = _normalize_docker_plan(result, authored_plan, req.python_version)
    return ResolutionResponse(
        docker_plan=docker_plan,
        docker_plan_status="available",
        prompts_issued=1,
    )
