import logging

import numpy as np
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"


class NewsEmbedder:

    def __init__(self):
        logger.info(f"[NewsEmbedder] loading model: {_MODEL_NAME}")
        self._model = SentenceTransformer(_MODEL_NAME)

    def encode(self, texts: list[str]) -> np.ndarray:
        return self._model.encode(texts, normalize_embeddings=True, show_progress_bar=False)

    def cluster(self, embeddings: np.ndarray, threshold: float = 0.85) -> list[list[int]]:
        sim_matrix = embeddings @ embeddings.T
        visited: set[int] = set()
        clusters: list[list[int]] = []

        for i in range(len(embeddings)):
            if i in visited:
                continue
            cluster = [i]
            visited.add(i)
            for j in range(i + 1, len(embeddings)):
                if j not in visited and sim_matrix[i][j] >= threshold:
                    cluster.append(j)
                    visited.add(j)
            clusters.append(cluster)

        return clusters
