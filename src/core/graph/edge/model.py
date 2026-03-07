from dataclasses import dataclass
from enum import StrEnum

from src.core.graph.node.model import Node

class EdgeType(StrEnum):
    DOCS_CONTRIBUTION = "docs_contribution"  # 문서 기여
    KNOWS = "knows"                 # 인물 → 인물
    INVOLVED_IN = "involved_in"     # 인물 → 사건/사고
    MEMBER_OF = "member_of"         # 인물 → 동아리

@dataclass
class Edge:
    type: EdgeType
    source: Node
    target: Node