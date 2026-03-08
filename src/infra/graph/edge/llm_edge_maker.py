from typing import List

from src.core.graph.edge.maker import GraphEdgeMaker
from src.core.graph.edge.model import Edge, EdgeType
from src.core.graph.node.model import Node, NodeRegistry
from src.core.llm import LLM
from src.dependencies.llm import get_llm
from src.infra.llm.output_parser.edge import EdgeOutputParser


_PROMPT_TEMPLATE = """다음은 교내 문서입니다.

문서 제목: {title}
문서 내용:
{content}

이미 발견된 관계 (참고용):
{known_edges_summary}

위 관계 외에도 본문에서 발견할 수 있는 추가 관계를 포함해서, 등장하는 인물·사건/사고·동아리와 문서 주인공의 관계를 분석하세요.
사용 가능한 관계 타입:
- {knows}: 인물 → 인물
- {involved_in}: 인물 → 사건/사고
- {member_of}: 인물 → 동아리

규칙:
- "target"은 반드시 위키 문서 제목 그대로 사용하세요. (예: "이세준", "방송부" 처럼 이름만, "선생님", "학생" 등의 호칭은 제외)
- 문서 제목이 명확하지 않은 관계는 포함하지 마세요.

아래 JSON 형식으로만 응답하세요:
[{{"target": "문서제목", "type": "관계타입"}}, ...]"""

class LLMEdgeMaker(GraphEdgeMaker):

    def __init__(
            self,
            source: Node,
            node_registry: NodeRegistry,
            llm: LLM = None,
            known_edges: List[Edge] = None,
    ):
        self._source = source
        self._node_registry = node_registry
        self._llm = llm or get_llm(template=_PROMPT_TEMPLATE)
        self._known_edges = known_edges or []
        self._parser = EdgeOutputParser(source=source, node_registry=node_registry)

    async def make(self, content: str) -> List[Edge]:
        known_summary = "\n".join(
            f"- {e.source.title} --[{e.type}]--> {e.target.title}"
            for e in self._known_edges
        ) or "없음"

        response = await self._llm.chat(
            query=_PROMPT_TEMPLATE,
            variables=[
                self._source.title,
                content,
                known_summary,
                EdgeType.KNOWS,
                EdgeType.INVOLVED_IN,
                EdgeType.MEMBER_OF,
            ],
        )

        edges = await self._parser.aparse(response)
        return edges
