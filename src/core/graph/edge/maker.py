from abc import ABC, abstractmethod
from typing import List

from src.core.graph.edge.model import Edge


class GraphEdgeMaker(ABC):

    @abstractmethod
    async def make(
            self,
            content : str
    ) -> List[Edge]:
        raise NotImplementedError