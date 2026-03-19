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
        n = len(embeddings)
        sim_matrix = embeddings @ embeddings.T

        parent = list(range(n))

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for i in range(n):
            for j in range(i + 1, n):
                if sim_matrix[i][j] >= threshold:
                    parent[find(i)] = find(j)

        groups: dict[int, list[int]] = {}
        for i in range(n):
            root = find(i)
            groups.setdefault(root, []).append(i)

        return list(groups.values())
