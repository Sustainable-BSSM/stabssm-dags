import argparse
import asyncio
import json
import logging
from typing import List

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from src.common.enum.bumawiki.docs_type import BumaWikiDocsType
from src.core.graph.edge.model import Edge, EdgeType
from src.core.graph.node.model import Node, NodeRegistry, NodeType
from src.core.jobs import Job
from src.core.repository.bumawiki.docs import BumaWikiDocsRepository
from src.core.repository.bumawiki.graph import BumaWikiGraphRepository

logger = logging.getLogger(__name__)
from src.dependencies.repository.bumawiki_docs import get_bumawiki_docs_repository
from src.dependencies.repository.bumawiki_graph import get_bumawiki_graph_repository
from src.infra.graph.edge.link_edge_maker import LinkEdgeMaker
from src.infra.graph.edge.llm_edge_maker import LLMEdgeMaker

_DOCS_TYPE_TO_NODE_TYPE = {
    BumaWikiDocsType.STUDENT: NodeType.PERSON,
    BumaWikiDocsType.TEACHER: NodeType.PERSON,
    BumaWikiDocsType.ACCIDENT: NodeType.ACCIDENT,
    BumaWikiDocsType.CLUB: NodeType.CLUB,
}


def _dedup_edges(edges: List[Edge]) -> List[Edge]:
    seen = set()
    result = []
    for edge in edges:
        key = (edge.type, edge.source.id, edge.target.id)
        if key not in seen:
            seen.add(key)
            result.append(edge)
    return result


def _row_to_node(row: dict) -> Node:
    docs_type = BumaWikiDocsType(row["docstype"].lower())
    return Node(
        id=row["id"],
        title=row["title"],
        type=_DOCS_TYPE_TO_NODE_TYPE[docs_type],
        docs_type=docs_type,
        last_modified_at=row["lastmodifiedat"],
        enroll=row.get("enroll", 0),
    )


async def _build_edges_for_node(
        node: Node,
        content: str,
        registry: NodeRegistry,
) -> List[Edge]:
    # 1단계: 링크 태그 기반 edge 추출
    link_maker = LinkEdgeMaker(source=node, node_registry=registry)
    link_edges = await link_maker.make(content)

    # 2단계: LLM이 링크 결과를 컨텍스트로 받아 추가 edge 발굴
    llm_maker = LLMEdgeMaker(
        source=node,
        node_registry=registry,
        known_edges=link_edges,
    )
    llm_edges = await llm_maker.make(content)

    return _dedup_edges(link_edges + llm_edges)


class BuildDocsGraphJob(Job):

    def __init__(
            self,
            repository: BumaWikiDocsRepository,
            graph_repository: BumaWikiGraphRepository,
    ):
        self._repository = repository
        self._graph_repository = graph_repository

    def __call__(self, ds: str):
        asyncio.run(self._run(ds))

    async def _run(self, ds: str):
        logger.info(f"[BuildDocsGraphJob] start ds={ds}")

        # 1. silver parquet 로드
        df = await self._repository.get(ds)
        if df.is_empty():
            logger.warning(f"[BuildDocsGraphJob] silver df is empty for ds={ds}, skipping")
            return
        logger.info(f"[BuildDocsGraphJob] columns={df.columns}")
        rows = df.to_dicts()

        # 2. 노드 생성 + NodeRegistry 구성
        nodes: List[Node] = []
        row_map: dict[str, dict] = {}
        for row in rows:
            node = _row_to_node(row)
            nodes.append(node)
            row_map[node.title] = row

        registry = NodeRegistry(nodes={n.title: n for n in nodes})

        # 3. 노드별 edge 수집
        all_edges: List[Edge] = []
        for node in nodes:
            row = row_map[node.title]
            content = row.get("contents", "")

            # 링크 기반 + LLM 기반 edge
            edges = await _build_edges_for_node(node, content, registry)
            all_edges.extend(edges)

            # DOCS_CONTRIBUTION: contributors → 문서 노드
            contributors: List[str] = json.loads(row.get("contributors", "[]"))
            for contributor_title in contributors:
                contributor = await registry.get_node(contributor_title)
                if contributor is None:
                    continue
                all_edges.append(Edge(
                    type=EdgeType.DOCS_CONTRIBUTION,
                    source=contributor,
                    target=node,
                ))

        all_edges = _dedup_edges(all_edges)

        # 4. parquet 직렬화 + 업로드
        logger.info(f"[BuildDocsGraphJob] saving {len(nodes)} nodes, {len(all_edges)} edges for ds={ds}")
        self._graph_repository.save_nodes(nodes, ds)
        self._graph_repository.save_edges(all_edges, ds)
        logger.info(f"[BuildDocsGraphJob] done")


def run_job(ds: str):
    repository = get_bumawiki_docs_repository()
    graph_repository = get_bumawiki_graph_repository()
    job = BuildDocsGraphJob(repository=repository, graph_repository=graph_repository)
    job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
