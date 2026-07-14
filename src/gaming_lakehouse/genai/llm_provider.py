"""Provider-agnostic LLM + embedding client. Vertex AI (Gemini) on GCP, Azure OpenAI on Azure.

Call sites never import a vendor SDK. Swapping clouds is a config change, not a refactor.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from functools import lru_cache

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.secrets import get_secret

log = get_logger(__name__)


class LLMProvider(ABC):
    @abstractmethod
    def complete(
        self,
        prompt: str,
        *,
        system: str = "",
        max_tokens: int = 1024,
        temperature: float = 0.2,
        json_mode: bool = False,
    ) -> str: ...

    @abstractmethod
    def embed(self, texts: list[str]) -> list[list[float]]: ...


class VertexProvider(LLMProvider):
    def __init__(self) -> None:
        import vertexai  # type: ignore
        from vertexai.generative_models import GenerativeModel  # type: ignore
        from vertexai.language_models import TextEmbeddingModel  # type: ignore

        settings = load_settings()
        vertexai.init(location=settings.get("beam.region", "us-central1"))
        self._chat = GenerativeModel(settings.get("ai.llm_model", "gemini-2.0-flash"))
        self._embed = TextEmbeddingModel.from_pretrained(settings.get("ai.embedding_model"))

    def complete(self, prompt, *, system="", max_tokens=1024, temperature=0.2, json_mode=False) -> str:
        config = {"max_output_tokens": max_tokens, "temperature": temperature}
        if json_mode:
            config["response_mime_type"] = "application/json"
        response = self._chat.generate_content(
            f"{system}\n\n{prompt}" if system else prompt, generation_config=config
        )
        return str(response.text)

    def embed(self, texts: list[str]) -> list[list[float]]:
        # Vertex caps the batch; chunking here keeps the call site oblivious.
        out: list[list[float]] = []
        for start in range(0, len(texts), 250):
            out.extend(e.values for e in self._embed.get_embeddings(texts[start : start + 250]))
        return out


class AzureOpenAIProvider(LLMProvider):
    def __init__(self) -> None:
        from openai import AzureOpenAI  # type: ignore

        settings = load_settings()
        self._client = AzureOpenAI(
            azure_endpoint=get_secret("azure-openai-endpoint"),
            api_key=get_secret("azure-openai-key"),
            api_version="2024-10-21",
        )
        self._chat_model = settings.get("ai.llm_model", "gpt-4o-mini")
        self._embed_model = settings.get("ai.embedding_model", "text-embedding-3-large")

    def complete(self, prompt, *, system="", max_tokens=1024, temperature=0.2, json_mode=False) -> str:
        messages = ([{"role": "system", "content": system}] if system else []) + [
            {"role": "user", "content": prompt}
        ]
        response = self._client.chat.completions.create(
            model=self._chat_model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            response_format={"type": "json_object"} if json_mode else {"type": "text"},
        )
        return response.choices[0].message.content or ""

    def embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for start in range(0, len(texts), 512):
            batch = self._client.embeddings.create(model=self._embed_model, input=texts[start : start + 512])
            out.extend(item.embedding for item in batch.data)
        return out


@lru_cache(maxsize=1)
def get_provider() -> LLMProvider:
    provider = load_settings().get("ai.llm_provider")
    log.info("llm provider selected", extra={"extra_fields": {"provider": provider}})
    return VertexProvider() if provider == "vertex" else AzureOpenAIProvider()
