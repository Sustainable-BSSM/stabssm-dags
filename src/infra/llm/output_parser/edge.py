import json
from typing import List

from langchain_core.output_parsers import BaseOutputParser

from src.core.graph.edge.model import Edge, EdgeTarget
from src.core.graph.node.model import Node, NodeRegistry


class EdgeOutputParser(
    BaseOutputParser[
        List[Edge]
    ]
):
    source: Node
    node_registry: NodeRegistry

    model_config = {"arbitrary_types_allowed": True}

    def parse(self, text: str) -> List[Edge]:
        raise NotImplementedError("Use aparse for async node resolution")

    async def aparse(self, text: str) -> List[Edge]:
        try:
            items = [EdgeTarget(**item) for item in json.loads(text)]
        except Exception:
            return []

        edges = []
        for item in items:
            target = await self.node_registry.get_node(item.target)
            if target is None:
                continue
            edges.append(Edge(type=item.type, source=self.source, target=target))
        return edges
