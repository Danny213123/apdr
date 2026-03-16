"""Shared LLM invocation utility for all agents.

Supports Ollama (via langchain-community) and OpenAI (via langchain-openai).
Uses LangChain's structured output parsing with Pydantic schemas.
"""

from __future__ import annotations

import os
from typing import Optional, Type, TypeVar

from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def _get_model(config: dict):
    """Create the appropriate LangChain chat model from config."""
    provider = config.get("llm_provider", "ollama")
    model_name = config.get("llm_model", "gemma3:4b")
    base_url = config.get("llm_base_url", "http://localhost:11434")

    if "gpt" in model_name.lower() or provider == "openai":
        from langchain_openai import ChatOpenAI

        api_key = os.environ.get("OPENAI_KEY") or os.environ.get("OPENAI_API_KEY", "")
        return ChatOpenAI(model=model_name, api_key=api_key, temperature=0.3)
    else:
        from langchain_community.chat_models import ChatOllama

        return ChatOllama(
            base_url=base_url,
            model=model_name,
            format="json",
            temperature=0.3,
        )


def llm_invoke(
    template: str,
    schema: Type[T],
    config: dict,
    max_retries: int = 3,
    **kwargs: str,
) -> Optional[T]:
    """Invoke LLM with a prompt template and parse structured output.

    Args:
        template: Prompt template string with {placeholders}
        schema: Pydantic model class for output validation
        config: Agent config dict with llm_provider, llm_model, llm_base_url
        max_retries: Number of retries on parse failure
        **kwargs: Variables to fill in the template

    Returns:
        Parsed Pydantic model instance, or None if LLM is unavailable/all retries fail
    """
    try:
        model = _get_model(config)
    except Exception:
        return None

    parser = JsonOutputParser(pydantic_object=schema)
    kwargs["format_instructions"] = parser.get_format_instructions()

    prompt = PromptTemplate(
        template=template,
        input_variables=[],
        partial_variables=kwargs,
    )

    chain = prompt | model | parser

    for attempt in range(max_retries):
        try:
            raw = chain.invoke({})
            # JsonOutputParser returns a dict; validate with Pydantic
            if isinstance(raw, dict):
                return schema(**raw)
            return raw
        except Exception:
            if attempt == max_retries - 1:
                return None
            continue

    return None
