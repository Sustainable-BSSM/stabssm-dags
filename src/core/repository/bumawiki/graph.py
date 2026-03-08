from abc import ABC, abstractmethod
from typing import List

from src.core.graph.edge.model import Edge
from src.core.graph.node.model import Node


class BumaWikiGraphRepository(ABC):

    @abstractmethod
    def save_nodes(self, nodes: List[Node], ds: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def save_edges(self, edges: List[Edge], ds: str) -> None:
        raise NotImplementedError
