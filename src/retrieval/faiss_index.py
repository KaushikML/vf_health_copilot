"""Optional FAISS-backed local vector index for fact retrieval."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass
class VectorDocument:
    """A searchable fact document."""

    text: str
    metadata: dict[str, Any]


class FactVectorIndex:
    """Simple local vector index with optional FAISS acceleration."""

    def __init__(self, dimension: int = 256, use_faiss_if_available: bool = True) -> None:
        self.dimension = dimension
        self.documents: list[VectorDocument] = []
        self.vectors: list[list[float]] = []
        self.index: Any | None = None
        self.use_faiss_if_available = use_faiss_if_available

        try:
            import faiss  # type: ignore
            import numpy as np  # type: ignore

            self._faiss = faiss
            self._np = np
        except Exception:
            self._faiss = None
            self._np = None

    def _tokenize(self, text: str) -> list[str]:
        return [token for token in "".join(ch.lower() if ch.isalnum() else " " for ch in text).split() if token]

    def _embed_text(self, text: str) -> list[float]:
        vector = [0.0] * self.dimension
        for token in self._tokenize(text):
            token_hash = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16)
            vector[token_hash % self.dimension] += 1.0
        # Vectors are L2-normalized here so later inner-product scoring behaves
        # like cosine similarity for both local fallback search and FAISS search.
        norm = math.sqrt(sum(value * value for value in vector))
        if norm == 0:
            return vector
        return [value / norm for value in vector]

    def build(self, documents: list[VectorDocument]) -> None:
        """Build the index from a list of documents."""

        self.documents = documents
        self.vectors = [self._embed_text(document.text) for document in documents]
        if self._faiss is None or self._np is None or not self.use_faiss_if_available:
            self.index = None
            return

        matrix = self._np.array(self.vectors, dtype="float32")
        # IndexFlatIP is used with normalized vectors so FAISS inner product
        # reproduces cosine-similarity behavior without changing retrieval logic.
        self.index = self._faiss.IndexFlatIP(self.dimension)
        self.index.add(matrix)

    @staticmethod
    def _metadata_matches(metadata: dict[str, Any], filters: dict[str, Any] | None) -> bool:
        if not filters:
            return True
        for key, value in filters.items():
            if value in (None, ""):
                continue
            if metadata.get(key) != value:
                return False
        return True

    @staticmethod
    def _dot(left: list[float], right: list[float]) -> float:
        return sum(a * b for a, b in zip(left, right))

    def search(self, query: str, top_k: int = 5, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Search the vector index and return scored documents."""

        query_vector = self._embed_text(query)
        candidate_indices = [
            index
            for index, document in enumerate(self.documents)
            if self._metadata_matches(document.metadata, filters)
        ]
        if not candidate_indices:
            return []

        scored = [
            {
                "score": round(self._dot(query_vector, self.vectors[index]), 4),
                "text": self.documents[index].text,
                "metadata": self.documents[index].metadata,
            }
            for index in candidate_indices
        ]
        scored.sort(key=lambda row: row["score"], reverse=True)
        return scored[:top_k]

    def save(self, directory: str | Path) -> None:
        """Persist documents and vectors to disk."""

        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        payload = {
            "dimension": self.dimension,
            "documents": [asdict(document) for document in self.documents],
            "vectors": self.vectors,
        }
        (target / "vector_index.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, directory: str | Path) -> "FactVectorIndex":
        """Restore a saved index from disk."""

        payload = json.loads((Path(directory) / "vector_index.json").read_text(encoding="utf-8"))
        index = cls(dimension=payload["dimension"])
        index.documents = [VectorDocument(**document) for document in payload["documents"]]
        index.vectors = payload["vectors"]
        return index
