import asyncio
from datetime import datetime

from src.common.enum.bumawiki.docs_type import BumaWikiDocsType
from src.core.graph.node.model import Node, NodeRegistry, NodeType
from src.infra.graph.edge.llm_edge_maker import LLMEdgeMaker

_rows = [
    (196, "강은수", "2023-10-10T01:44:17", 2022),
    (197, "방소연", "2023-04-04T22:06:49", 2022),
    (198, "이경숙", "2024-03-19T18:01:52", 2022),
    (199, "김규봉", "2024-04-04T21:18:03", 2022),
    (202, "구진영", "2024-05-29T14:33:25", 2022),
    (203, "김진필", "2024-11-14T09:24:11", 2022),
    (205, "차수민", "2024-06-21T11:28:35", 2022),
    (215, "최병준", "2024-04-23T12:02:00", 2022),
    (300, "이세준", "2023-01-01T00:00:00", 2021),
]

nodes = {
    title: Node(
        id=id_,
        title=title,
        type=NodeType.PERSON,
        docs_type=BumaWikiDocsType.TEACHER,
        last_modified_at=datetime.fromisoformat(ts),
        enroll=enroll,
    )
    for id_, title, ts, enroll in _rows
}
registry = NodeRegistry(nodes=nodes)

source = nodes["구진영"]
content = """부산소마고의 전기전자기초/마이크로프로세서/컴퓨터구조 등 임베디드 시스템 관련 선생님.
이세준 선생님과 친하기로 유명하다.
이경숙 선생님의 수업에서 짝 쪽지 조 짜기를 했는데 구진영 선생님과 이세준 선생님이라고 적힌 쪽지가 세 쌍이나 나왔다."""

maker = LLMEdgeMaker(source=source, node_registry=registry)


async def main():
    print(f"[source] {source.title}")
    print(f"[registry] {list(nodes.keys())}")
    print(f"[content]\n{content}\n")

    print("LLM 호출 중...")
    edges = await maker.make(content)

    print(f"\n[결과] edge {len(edges)}개")
    for e in edges:
        print(f"  {e.source.title} --[{e.type}]--> {e.target.title}")


if __name__ == "__main__":
    asyncio.run(main())

# 테스트 결과
# """
# [결과] edge 1개
#   구진영 --[knows]--> 이세준
# """
