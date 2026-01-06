import json
import requests
import re
import os
from html import unescape
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
import libsql_client


class TagesschauClient:
    _HTML_TAG_RE = re.compile(r"<[^>]+>")

    def __init__(
        self,
        api_config_path: str,
        regions_path: str,
        source_regions_path: str,
        url_region_keywords_path: str,
        filters_path: str,
        table_name: str = "articles",
        connect_db: bool = True,   # 👈 NEU
    ) -> None:
        # ----------------------------
        # Load configs
        # ----------------------------
        self.api_config = self._load_json(api_config_path)
        self.region_map = self._load_json(regions_path)
        self.source_region_map = self._load_json(source_regions_path)
        self.url_region_keywords = self._load_json(url_region_keywords_path)
        self.filters = self._load_json(filters_path)["exclude"]

        self.base_index_url = self.api_config["base_index_url"]
        self.base_detail_url = self.api_config["base_detail_url"]
        self.timeout = self.api_config.get("timeout", 10)

        self.table_name = table_name

        # ----------------------------
        # Turso config
        # ----------------------------
        self.turso_url = os.environ.get("TURSO_DB_URL")
        self.turso_token = os.environ.get("TURSO_AUTH_TOKEN")

        if connect_db:
            if not self.turso_url or not self.turso_token:
                raise RuntimeError("Missing TURSO_DB_URL or TURSO_AUTH_TOKEN")

        self._db = None

        self.last_ingest_date = None
        self.effective_published_after = None

    # ------------------------------------------------------------------
    # DB (Turso)
    # ------------------------------------------------------------------
    async def _connect(self):
        if self._db is None:
            http_url = self.turso_url.replace("libsql://", "https://")

            self._db = libsql_client.create_client(
                url=http_url,
                auth_token=self.turso_token,
            )
        return self._db

    async def _close(self):
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def _ensure_table(self):
        db = await self._connect()
        await db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                external_id TEXT PRIMARY KEY,
                sophora_id TEXT,
                title TEXT,
                published_at TEXT,
                ressort TEXT,
                type TEXT,
                url TEXT,
                source TEXT,
                region_by_api TEXT,
                region_by_source TEXT,
                region_by_url TEXT,
                subregion_by_url TEXT,
                meta_infos_multiple TEXT,
                fulltext TEXT NOT NULL,
                ingest_date TEXT NOT NULL
            )
            """
        )

    async def _get_last_ingest_date(self) -> Optional[str]:
        try:
            db = await self._connect()
            rs = await db.execute(
                f"SELECT MAX(ingest_date) AS d FROM {self.table_name}"
            )
            row = rs.rows[0] if rs.rows else None
            return row["d"] if row and row["d"] else None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # IO
    # ------------------------------------------------------------------
    @staticmethod
    def _load_json(path: str) -> Dict:
        path = Path(path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    # ------------------------------------------------------------------
    # Fetching
    # ------------------------------------------------------------------
    def fetch_index(self) -> List[Dict]:
        resp = requests.get(self.base_index_url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()["news"]

    def fetch_story_details(self, sophora_id: str) -> Dict:
        url = self.base_detail_url.format(sophora_id=sophora_id)
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Text extraction
    # ------------------------------------------------------------------
    def _clean_html(self, text: str) -> str:
        text = unescape(text)
        text = self._HTML_TAG_RE.sub("", text)
        return text.strip()

    def extract_fulltext(self, details: Dict) -> str:
        parts: List[str] = []

        for block in details.get("content", []):
            if block.get("type") in {"text", "headline"} and "value" in block:
                cleaned = self._clean_html(block["value"])
                if cleaned:
                    parts.append(cleaned)

        return "\n\n".join(parts)

    # ------------------------------------------------------------------
    # Metadata logic (SINGLE SOURCE OF TRUTH)
    # ------------------------------------------------------------------
    def _region_by_api(self, region_ids: Optional[List[int]]) -> str:
        if not region_ids:
            return self.region_map["null"]

        names = [self.region_map.get(str(r)) for r in region_ids if str(r) in self.region_map]
        names = [n for n in names if n]
        return ", ".join(names) if names else self.region_map["null"]

    def _source_from_url(self, url: Optional[str]) -> str:
        if not url:
            return "unknown"

        host = urlparse(url).netloc.lower()
        host = host.replace("www.", "").replace("www1.", "")
        return host.split(".")[0] if host else "unknown"

    def _region_by_source(self, source: str) -> str:
        return self.source_region_map.get(source, self.source_region_map.get("unknown", "Unbekannt"))

    def _region_by_url(self, url: Optional[str]) -> Tuple[str, Optional[str]]:
        if not url:
            return "Bundesweit", None

        path = urlparse(url).path.lower()

        bundesland = None
        matched_key = None

        for key, value in self.url_region_keywords.items():
            if f"/{key}/" in path:
                bundesland = value
                matched_key = key
                break

        if not bundesland or not matched_key:
            return "Bundesweit", None

        segments = path.strip("/").split("/")
        if matched_key not in segments:
            return bundesland, None

        idx = segments.index(matched_key)

        subregion = None
        if idx + 1 < len(segments):
            candidate = segments[idx + 1]
            if "_" in candidate or "-" not in candidate:
                subregion = candidate.replace("_", " ").split("-")[0].title()

        return bundesland, subregion

    # 🔥 ZENTRALER HELPER FÜR INGEST + POST-CLEANUP
    def recompute_metadata_from_url(self, url: Optional[str]) -> Tuple[str, str, str, Optional[str]]:
        source = self._source_from_url(url)
        region_by_source = self._region_by_source(source)
        region_by_url, subregion_by_url = self._region_by_url(url)
        return source, region_by_source, region_by_url, subregion_by_url

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------
    def normalize_article(self, index_article: Dict, details: Dict) -> Dict:
        url = index_article.get("shareURL")

        source, region_by_source, region_by_url, subregion_by_url = self.recompute_metadata_from_url(url)

        return {
            "external_id": index_article.get("externalId"),
            "sophora_id": index_article.get("sophoraId"),
            "title": index_article.get("title"),
            "published_at": index_article.get("date"),
            "ressort": index_article.get("ressort"),
            "type": index_article.get("type"),
            "url": url,
            "source": source,
            "region_by_api": self._region_by_api(index_article.get("regions")),
            "region_by_source": region_by_source,
            "region_by_url": region_by_url,
            "subregion_by_url": subregion_by_url,
            "meta_infos_multiple": json.dumps({}, ensure_ascii=False),
            "fulltext": self.extract_fulltext(details),
        }

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------
    async def collect_and_store(self) -> None:
        await self._ensure_table()

        self.last_ingest_date = await self._get_last_ingest_date()
        self.effective_published_after = (
            self.last_ingest_date
            or self.api_config.get("published_after")
        )

        print(
            f"🕒 Ingest watermark (from ingest_date): "
            f"{self.effective_published_after or 'None'}"
        )

        stats = {
            "api_returned": 0,
            "eligible": 0,
            "filtered_type": 0,
            "filtered_ressort": 0,
            "filtered_watermark": 0,
            "inserted": 0,
            "no_fulltext": 0,
            "failed": 0,
        }

        ingest_ts = datetime.utcnow().isoformat(timespec="seconds")

        db = await self._connect()

        try:
            for article in self.fetch_index():
                stats["api_returned"] += 1

                if article.get("type") in self.filters["types"]:
                    stats["filtered_type"] += 1
                    continue

                if article.get("ressort") in self.filters["ressorts"]:
                    stats["filtered_ressort"] += 1
                    continue

                if self.effective_published_after:
                    article_date = article.get("date")
                    if article_date and article_date <= self.effective_published_after:
                        stats["filtered_watermark"] += 1
                        continue

                stats["eligible"] += 1

                try:
                    details = self.fetch_story_details(article["sophoraId"])
                    record = self.normalize_article(article, details)

                    if not record["fulltext"]:
                        stats["no_fulltext"] += 1
                        continue

                    record["ingest_date"] = ingest_ts

                    cols = ", ".join(record.keys())
                    placeholders = ", ".join(["?"] * len(record))

                    await db.execute(
                        f"""
                        INSERT OR IGNORE INTO {self.table_name}
                        ({cols}) VALUES ({placeholders})
                        """,
                        list(record.values()),
                    )

                    stats["inserted"] += 1

                except Exception as e:
                    print("ERROR:", e)
                    stats["failed"] += 1

        finally:
            await self._close()

        print("\n📊 TAGESSCHAU INGEST SUMMARY")
        print(f"🔹 Artikel von API (Index): {stats['api_returned']}")
        print(f"🕒 Nach Watermark relevant: {stats['eligible']}")
        print(f"💾 Artikel gespeichert:     {stats['inserted']}")
        print(f"📄 Kein Fulltext:           {stats['no_fulltext']}")
        print(f"❌ Fehlgeschlagen:          {stats['failed']}")
        print(f"⏭️ Gefiltert (Typ):         {stats['filtered_type']}")
        print(f"⏭️ Gefiltert (Ressort):     {stats['filtered_ressort']}")
        print(f"⏭️ Gefiltert (Watermark):   {stats['filtered_watermark']}")
