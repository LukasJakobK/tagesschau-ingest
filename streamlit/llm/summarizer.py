from openai import OpenAI



class OpenAISummarizer:
    def __init__(self, api_key: str, prompts: dict, prompt_key: str, topic: str | None = None, model: str = "gpt-4.1-mini"):
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.topic = topic.strip() if topic else ""

        if prompt_key not in prompts:
            raise ValueError(f"Prompt key '{prompt_key}' not found in prompts JSON")

        self.system_template = prompts[prompt_key]["system"]
        self.user_template = prompts[prompt_key]["user"]
        

    def summarize(self, query: str, documents: list[dict]) -> dict:
        """
        Returns a structured dict:
        {
          "summary": "...",
          "claims": [{"text": "...", "sources": [1,2]}],
          "raw": "<model output>"
        }
        If parsing fails, claims may be empty.
        """
        blocks = []
        for i, doc in enumerate(documents, 1):
            blocks.append(
                f"[{i}]\n"
                f"Titel: {doc.get('title','')}\n"
                f"Datum: {doc.get('published_at','')}\n"
                f"URL: {doc.get('url','')}\n"
                f"Inhalt:\n{doc.get('text','')}\n"
            )
        documents_text = "\n\n".join(blocks)

        system_prompt = (
        f"TOPIC (FIXED – NICHT ÄNDERN): {self.topic}\n\n" + self.system_template)
        user_prompt = self.user_template.format(query=query,topic=self.topic, documents=documents_text)

        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )

        raw = (resp.choices[0].message.content or "").strip()

        # Minimal, robust extraction: look for "ZUSAMMENFASSUNG:" and "AUSSAGEN:"
        summary_text = raw
        claims: list[dict] = []

        def _section(name: str) -> str:
            pat = rf"{name}\s*:\s*"
            m = __import__("re").search(pat, raw, flags=__import__("re").IGNORECASE)
            if not m:
                return ""
            start = m.end()
            # end at next ALLCAPS section label
            m2 = __import__("re").search(r"\n[A-ZÄÖÜ][A-ZÄÖÜ \-]{2,}:\s*\n", raw[start:], flags=__import__("re").MULTILINE)
            end = start + (m2.start() if m2 else len(raw[start:]))
            return raw[start:end].strip()

        sec_summary = _section("ZUSAMMENFASSUNG")
        sec_claims = _section("AUSSAGEN")

        if sec_summary:
            summary_text = sec_summary

        if sec_claims:
            # Parse lines like: "- Text ... [1,2]"
            import re

            for line in sec_claims.splitlines():
                line = line.strip()
                if not line:
                    continue
                if line.startswith("-"):
                    line = line[1:].strip()
                m = re.search(r"\[(.*?)\]\s*$", line)
                if m:
                    src_raw = m.group(1)
                    src = []
                    for part in src_raw.split(","):
                        part = part.strip()
                        if part.isdigit():
                            src.append(int(part))
                    text = re.sub(r"\s*\[(.*?)\]\s*$", "", line).strip()
                    if text:
                        claims.append({"text": text, "sources": src})
                else:
                    claims.append({"text": line, "sources": []})

        return {"summary": summary_text.strip(), "claims": claims, "raw": raw}

