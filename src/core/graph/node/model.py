from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Dict

from src.common.enum.bumawiki.docs_type import BumaWikiDocsType


class NodeType(StrEnum):
    PERSON = "person"
    ACCIDENT = "accident"
    CLUB = "club"


@dataclass
class Node:
    title : str
    id : int
    type : NodeType
    docs_type : BumaWikiDocsType
    last_modified_at : datetime
    enroll : int

@dataclass
class NodeRegistry:
    nodes : Dict[str, Node] # [node title : node]

    async def get_node(self, title: str) -> Node | None:
        return self.nodes.get(title)
