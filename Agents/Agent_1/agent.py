import json
import re
from pathlib import Path
from typing import Any

from .config.schema import SearchPlan


class RetrievalAgent:
    """
    Agent 1 – Retrieval / Search Planning
    Erwartet prompt.json als LISTE von OpenAI-Messages.
    """

    def __init__(
        self,
        llm_client,
        prompts_path: str | Path | None = None,
        model: str = "gpt-4.1-mini",
    ):
        self.llm = llm_client
        self.model = model

        if prompts_path is None:
            base_dir = Path(__file__).resolve().parent
            prompts_path = base_dir / "config" / "prompt.json"

        self.prompt_messages = self._load_prompts(prompts_path)

    # ------------------------------------------------------------

    @staticmethod
    def _load_prompts(path: str | Path) -> list[dict]:
        path = Path(path)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise TypeError("prompt.json muss eine LISTE von Messages sein")

        return data

    # ------------------------------------------------------------

    def _build_messages(self, question: str) -> list[dict]:
        return [
            {
                "role": msg["role"],
                "content": msg["content"].replace(
                    "{USER_QUESTION}", question
                ),
            }
            for msg in self.prompt_messages
        ]

    # ------------------------------------------------------------
    # 🔒 NORMALISIERUNG
    # ------------------------------------------------------------

    @staticmethod
    def _normalize_plan(data: dict) -> dict:
        filters = data.get("filters", {})

        # ---------- published_at ----------
        if "published_at" in filters:
            p = filters["published_at"]

            if isinstance(p, str):
                p = p.strip()
                if p.startswith(">="):
                    filters["published_at"] = {"gte": p[2:].strip()}
                elif p.startswith(">"):
                    filters["published_at"] = {"gte": p[1:].strip()}
                elif p.startswith("<="):
                    filters["published_at"] = {"lte": p[2:].strip()}
                elif p.startswith("<"):
                    filters["published_at"] = {"lte": p[1:].strip()}
                else:
                    filters["published_at"] = {"gte": p}

            elif isinstance(p, dict):
                if "from" in p and "gte" not in p:
                    p["gte"] = p.pop("from")
                if "to" in p and "lte" not in p:
                    p["lte"] = p.pop("to")

        # ---------- list fields ----------
        for field in [
            "region_by_url",
            "subregion_by_url",
            "region_by_api",
            "region_by_source",
            "source",
            "ressort",
            "type",
        ]:
            val = filters.get(field)
            if isinstance(val, str):
                filters[field] = [val]

        data["filters"] = filters

        # ---------- queries ----------
        normalized_queries = []
        for q in data.get("queries", []):
            if "text" not in q and "query" in q:
                normalized_queries.append(
                    {
                        "text": q["query"],
                        "weight": q.get("weight", 1.0),
                    }
                )
            else:
                normalized_queries.append(q)

        data["queries"] = normalized_queries

        return data

    # ------------------------------------------------------------

    def plan(self, question: str) -> SearchPlan:
        messages = self._build_messages(question)

        response = self.llm.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0.0,
        )

        raw_text = response.choices[0].message.content or ""

        # ---------- JSON extrahieren ----------
        match = re.search(r"\{.*\}", raw_text, flags=re.DOTALL)
        if not match:
            raise ValueError("Kein JSON im LLM-Output:\n" + raw_text)

        data: dict[str, Any] = json.loads(match.group(0))

        # ---------- NORMALISIEREN ----------
        data = self._normalize_plan(data)

        # ---------- VALIDIEREN ----------
        return SearchPlan.model_validate(data)
