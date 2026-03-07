import argparse
import asyncio
import io
import json
from typing import List

import polars as pl

from src.common.config.s3 import S3Config
from src.common.enum.bumawiki.docs_type import BumaWikiDocsType
from src.core.client.storage import StorageClient
from src.core.graph.edge.model import Edge, EdgeType
from src.core.graph.node.model import Node, NodeRegistry, NodeType
from src.core.jobs import Job
from src.dependencies.storage_client import get_storage_client
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
    # bumawiki API 응답 필드명 기준 (camelCase)
    docs_type = BumaWikiDocsType(row["docsType"])
    return Node(
        id=row["id"],
        title=row["title"],
        type=_DOCS_TYPE_TO_NODE_TYPE[docs_type],
        docs_type=docs_type,
        last_modified_at=row["lastModifiedAt"],
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
            storage_client: StorageClient,
            bucket_name: str = S3Config.BUCKET_NAME,
    ):
        self._storage = storage_client
        self._bucket = bucket_name

    def __call__(self, ds: str):
        asyncio.run(self._run(ds))

    async def _run(self, ds: str):
        # 1. silver parquet 로드
        key = f"silver/bumawiki/docs/dt={ds}/part-0000.parquet"
        raw = self._storage.get_bytes(key)
        if not raw:
            return

        df = pl.read_parquet(io.BytesIO(raw))
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
            content = row.get("content", "")

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
        self._upload_nodes(nodes, ds)
        self._upload_edges(all_edges, ds)

    def _upload_nodes(self, nodes: List[Node], ds: str):
        rows = [
            {
                "id": n.id,
                "title": n.title,
                "type": n.type.value,
                "docs_type": n.docs_type.value,
                "last_modified_at": str(n.last_modified_at),
                "enroll": n.enroll,
            }
            for n in nodes
        ]
        buf = io.BytesIO()
        pl.DataFrame(rows).write_parquet(buf, compression="snappy")
        self._storage.upload_bytes(
            key=f"gold/bumawiki/docs/nodes/dt={ds}/part-0000.parquet",
            data=buf.getvalue(),
        )

    def _upload_edges(self, edges: List[Edge], ds: str):
        rows = [
            {
                "type": e.type.value,
                "source_id": e.source.id,
                "source_title": e.source.title,
                "target_id": e.target.id,
                "target_title": e.target.title,
            }
            for e in edges
        ]
        buf = io.BytesIO()
        pl.DataFrame(rows).write_parquet(buf, compression="snappy")
        self._storage.upload_bytes(
            key=f"gold/bumawiki/docs/edges/dt={ds}/part-0000.parquet",
            data=buf.getvalue(),
        )


def run_job(ds: str):
    storage_client = get_storage_client()
    job = BuildDocsGraphJob(storage_client=storage_client)
    job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
