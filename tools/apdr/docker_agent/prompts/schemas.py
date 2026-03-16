from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ConfidenceAssessment(BaseModel):
    decision: str = Field(description="'solve' or 'skip'")
    confidence: float = Field(description="0.0 to 1.0")
    reason: str = Field(description="Brief explanation")


class ErrorClassification(BaseModel):
    error_type: str = Field(
        description=(
            "VersionNotFound, DependencyConflict, ModuleNotFound, BuildFailure, "
            "ImportError, SyntaxError, PythonVersionMismatch, or Unknown"
        )
    )
    offending_package: str = Field(description="Package causing the error, or 'unknown'")
    detail: str = Field(description="Specific error detail")


class RecoveryAction(BaseModel):
    action: str = Field(
        description=(
            "change_version, add_package, remove_package, "
            "add_system_dep, try_next_python, or give_up"
        )
    )
    package: str = Field(description="Package to modify")
    version: Optional[str] = Field(default=None, description="New version to try")
    system_deps: Optional[list[str]] = Field(
        default=None, description="Apt packages to add"
    )
    reason: str = Field(description="Why this fix should work")
