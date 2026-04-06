"""High-level vector retrieval wrapper for long-form facility facts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.retrieval.faiss_index import FactVectorIndex, VectorDocument


def fact_row_to_text(row: dict[str, Any]) -> str:
    """Convert a long-form fact row into the retrieval document template."""

    return (
        f"{row.get('name')} | {row.get('region')} | {row.get('facilityTypeId')} | "
        f"{row.get('fact_type')} | {row.get('fact_text')}"
    )


@dataclass
class VectorRetriever:
    """Semantic retriever over exploded fact rows."""

    index: FactVectorIndex

    @classmethod
    def from_fact_rows(
        cls,
        fact_rows: list[dict[str, Any]],
        use_faiss_if_available: bool = True,
    ) -> "VectorRetriever":
        documents = [
            VectorDocument(
                text=fact_row_to_text(row),
                metadata={
                    "unique_id": row.get("unique_id"),
                    "name": row.get("name"),
                    "region": row.get("region"),
                    "fact_type": row.get("fact_type"),
                    "facilityTypeId": row.get("facilityTypeId"),
                    "operatorTypeId": row.get("operatorTypeId"),
                },
            )
            for row in fact_rows
        ]
        index = FactVectorIndex(use_faiss_if_available=use_faiss_if_available)
        index.build(documents)
        return cls(index=index)

    def search(self, query: str, filters: dict[str, Any] | None = None, top_k: int = 8) -> list[dict[str, Any]]:
        """Search the vector index and flatten the result structure."""

        results = self.index.search(query, top_k=top_k, filters=filters)
        return [{**result["metadata"], "score": result["score"], "text": result["text"]} for result in results]

    def save(self, directory: str | Path) -> None:
        """Persist the vector index to disk."""

        self.index.save(directory)

    @classmethod
    def load(cls, directory: str | Path) -> "VectorRetriever":
        """Load a vector index from disk."""

        return cls(index=FactVectorIndex.load(directory))
