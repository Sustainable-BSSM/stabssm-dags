from typing import List

import polars as pl

from src.common.config.s3 import S3Config
from src.core.graph.edge.model import Edge
from src.core.graph.node.model import Node
from src.core.repository.bumawiki.graph import BumaWikiGraphRepository
from src.infra.duckdb import create_conn


class DuckDBBumaWikiGraphRepository(BumaWikiGraphRepository):

    def __init__(self, bucket: str = S3Config.BUCKET_NAME):
        self._bucket = bucket
        self._conn = create_conn()

    def save_nodes(self, nodes: List[Node], ds: str) -> None:
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
        self._conn.register("nodes_df", pl.DataFrame(rows))
        self._conn.execute(f"""
            COPY nodes_df
            TO 's3://{self._bucket}/gold/bumawiki/docs/nodes/dt={ds}/part-0000.parquet'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)

    def save_edges(self, edges: List[Edge], ds: str) -> None:
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
        self._conn.register("edges_df", pl.DataFrame(rows))
        self._conn.execute(f"""
            COPY edges_df
            TO 's3://{self._bucket}/gold/bumawiki/docs/edges/dt={ds}/part-0000.parquet'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
