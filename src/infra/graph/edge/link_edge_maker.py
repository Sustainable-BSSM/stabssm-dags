import re
from typing import List

from src.core.graph.edge.maker import GraphEdgeMaker
from src.core.graph.edge.model import Edge
from src.core.graph.edge.model import EdgeType
from src.core.graph.node.model import Node, NodeType, NodeRegistry

_EDGE_TYPE_MAP = {
    NodeType.PERSON: EdgeType.KNOWS,
    NodeType.ACCIDENT: EdgeType.INVOLVED_IN,
    NodeType.CLUB: EdgeType.MEMBER_OF,
}


class LinkEdgeMaker(GraphEdgeMaker):

    def __init__(self, source: Node, node_registry: NodeRegistry):
        self._source = source
        self._node_registry = node_registry
        self._link_pattern = re.compile(r'<링크 문서=\{([^}]+)\}>[^<]*</링크>')

    async def make(self, content: str) -> List[Edge]:
        edges = []
        for match in self._link_pattern.finditer(content):
            target_title = match.group(1)
            target = await self._node_registry.get_node(target_title)
            if target is None:
                continue
            edge_type = _EDGE_TYPE_MAP.get(target.type)
            if edge_type is None:
                continue
            edges.append(Edge(type=edge_type, source=self._source, target=target))
        return edges
