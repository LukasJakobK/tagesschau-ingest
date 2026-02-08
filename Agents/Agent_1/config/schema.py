# rag/agent1/schema.py
from typing import List, Dict, Literal, Optional
from pydantic import BaseModel, Field

Strategy = Literal["vector", "hybrid", "metadata"]
SortMode = Literal["relevance", "time_desc"]


class QuerySpec(BaseModel):
    text: str
    weight: float = Field(ge=0.0)


class FilterSpec(BaseModel):
    published_at: Optional[Dict[str, str]] = None
    region_by_url: Optional[List[str]] = None
    subregion_by_url: Optional[List[str]] = None
    region_by_api: Optional[List[str]] = None
    region_by_source: Optional[List[str]] = None
    source: Optional[List[str]] = None
    ressort: Optional[List[str]] = None
    type: Optional[List[str]] = None


class SearchPlan(BaseModel):
    strategy: Strategy
    queries: List[QuerySpec]
    filters: FilterSpec
    top_k: int = 25
    sort: SortMode = "relevance"
    notes: str
