import logging

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

logger = logging.getLogger(__name__)


class NewsEmbedder:

    def __init__(self):
        self._vectorizer = TfidfVectorizer(
            analyzer="char_wb",
            ngram_range=(2, 3),
            max_features=8192,
            sublinear_tf=True,
        )

    def encode(self, texts: list[str]) -> np.ndarray:
        matrix = self._vectorizer.fit_transform(texts)
        norms = np.asarray(matrix.multiply(matrix).sum(axis=1)).flatten() ** 0.5
        norms[norms == 0] = 1
        return (matrix.multiply(1 / norms[:, None])).toarray()

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
