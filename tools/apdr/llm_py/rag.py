"""RAG and context-folding helpers for tier3 prompt assembly.

These helpers keep effective context focused on the current import set by
combining failure memory, reverse-index evidence, and benchmark context under a
named retrieval profile instead of growing the prompt indiscriminately.
"""

from __future__ import annotations

from typing import Any


def assemble_context_from_parts(
    context: list[str],
    failure_context: str = "",
) -> list[str]:
    """Combine context parts and optional failure memory context."""
    result = list(context)
    if failure_context:
        result.append(failure_context)
    return result


def _normalize_retrieval_profile(value: str) -> str:
    text = str(value or "").strip().lower()
    return text or "none"


def _dedupe_lines(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for line in lines:
        text = str(line or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _score_line(line: str, import_names: list[str]) -> int:
    lower = line.lower()
    score = 0
    for import_name in import_names:
        normalized = import_name.lower()
        if normalized and normalized in lower:
            score += 6
    if "previous failure" in lower or "failure memory" in lower:
        score += 4
    if "known package" in lower:
        score += 3
    if "reverse" in lower or "fuzzy" in lower:
        score += 2
    return score


def _select_context_lines(
    lines: list[str],
    import_names: list[str],
    retrieval_profile: str,
) -> list[str]:
    normalized = _normalize_retrieval_profile(retrieval_profile)
    if normalized == "none":
        return lines

    ranked = sorted(
        lines,
        key=lambda line: (_score_line(line, import_names), -len(line)),
        reverse=True,
    )
    limit = 12 if "fold" in normalized or "summary" in normalized else 20
    if "wide" in normalized:
        limit = 32
    return ranked[:limit]


def _fold_context_lines(lines: list[str], max_chars: int) -> list[str]:
    folded: list[str] = []
    used = 0
    for line in lines:
        candidate = f"- {line}"
        if used + len(candidate) > max_chars and folded:
            break
        folded.append(line)
        used += len(candidate)
    return folded


def _summarize_benchmark_context(benchmark_context: str, retrieval_profile: str) -> tuple[str, str]:
    from .prompts import compress_benchmark_context

    normalized = _normalize_retrieval_profile(retrieval_profile)
    if "summary" in normalized or "fold" in normalized or "memory" in normalized:
        max_chars = 4096
        strategy = "benchmark-summary"
    elif normalized == "none":
        max_chars = 12288
        strategy = "benchmark-full"
    else:
        max_chars = 8192
        strategy = "benchmark-trimmed"
    return compress_benchmark_context(benchmark_context, max_chars=max_chars), strategy


def assemble_retrieval_context(
    *,
    import_names: list[str],
    context: list[str],
    failure_context: str,
    benchmark_context: str,
    retrieval_profile: str,
) -> dict[str, Any]:
    """Assemble context, failure memory, reverse evidence, and summarized benchmark context."""
    normalized = _normalize_retrieval_profile(retrieval_profile)
    combined = assemble_context_from_parts(context, failure_context)
    deduped = _dedupe_lines(combined)
    selected = _select_context_lines(deduped, import_names, normalized)
    folded = _fold_context_lines(
        selected,
        max_chars=5000 if "fold" in normalized or "summary" in normalized else 9000,
    )
    summarized_benchmark_context, benchmark_strategy = _summarize_benchmark_context(
        benchmark_context,
        normalized,
    )
    notes = [
        (
            f"Retrieval context strategy `{normalized}` selected "
            f"{len(folded)} of {len(deduped)} context lines."
        ),
        f"Benchmark context strategy `{benchmark_strategy}` applied.",
    ]
    return {
        "context": folded,
        "benchmark_context": summarized_benchmark_context,
        "retrieval_profile": normalized,
        "notes": notes,
    }
