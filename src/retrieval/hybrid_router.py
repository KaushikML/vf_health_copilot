"""Routing between structured and vector retrieval paths."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.retrieval.sql_retriever import SQLRetriever, StructuredQuery
from src.retrieval.vector_retriever import VectorRetriever


@dataclass
class RetrievalRequest:
    """Combined retrieval request for the hybrid pipeline."""

    query_text: str
    query_family: str
    filters: dict[str, Any] = field(default_factory=dict)
    structured_limit: int = 25
    vector_limit: int = 8


class HybridRetrievalEngine:
    """Choose the best retrieval mode for each supported query family."""

    def __init__(self, sql_retriever: SQLRetriever, vector_retriever: VectorRetriever | None = None) -> None:
        self.sql_retriever = sql_retriever
        self.vector_retriever = vector_retriever

    def route(self, query_family: str) -> str:
        """Return the retrieval mode for a query family."""

        if query_family in {"count/ranking", "ngo analysis"}:
            return "structured"
        if query_family in {"unsupported", "external-data-needed"}:
            return "none"
        if query_family in {"facility lookup", "service search", "anomaly detection", "planner recommendation"}:
            return "hybrid" if self.vector_retriever is not None else "structured"
        if query_family == "region gap analysis":
            return "structured"
        return "structured"

    def retrieve(self, request: RetrievalRequest) -> dict[str, Any]:
        """Run the chosen structured/vector retrieval strategy."""

        route = self.route(request.query_family)
        structured_result = {"rows": [], "sql": "", "family": request.query_family, "filters": request.filters}
        vector_rows: list[dict[str, Any]] = []

        if route in {"structured", "hybrid"}:
            structured_result = self.sql_retriever.run(
                StructuredQuery(
                    family=request.query_family,
                    filters=request.filters,
                    limit=request.structured_limit,
                )
            )
        if route in {"vector", "hybrid"} and self.vector_retriever is not None:
            metadata_filters = {
                "region": request.filters.get("region"),
                "facilityTypeId": request.filters.get("facility_type"),
            }
            vector_rows = self.vector_retriever.search(
                request.query_text,
                filters=metadata_filters,
                top_k=request.vector_limit,
            )

        return {
            "retrieval_path": route,
            "structured": structured_result,
            "vector": vector_rows,
        }
