from typing import List

import polars as pl
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

from src.common.config.glue import GlueConfig
from src.common.config.s3 import S3Config
from src.core.graph.edge.model import Edge
from src.core.graph.node.model import Node
from src.core.repository.bumawiki.graph import BumaWikiGraphRepository
from src.infra.iceberg import create_catalog

_NODES_SCHEMA = Schema(
    NestedField(1, "id", StringType(), required=False),
    NestedField(2, "title", StringType(), required=False),
    NestedField(3, "type", StringType(), required=False),
    NestedField(4, "docs_type", StringType(), required=False),
    NestedField(5, "last_modified_at", StringType(), required=False),
    NestedField(6, "enroll", LongType(), required=False),
    NestedField(7, "ds", StringType(), required=False),
)

_EDGES_SCHEMA = Schema(
    NestedField(1, "type", StringType(), required=False),
    NestedField(2, "source_id", StringType(), required=False),
    NestedField(3, "source_title", StringType(), required=False),
    NestedField(4, "target_id", StringType(), required=False),
    NestedField(5, "target_title", StringType(), required=False),
    NestedField(6, "ds", StringType(), required=False),
)


class IcebergBumaWikiGraphRepository(BumaWikiGraphRepository):

    _NODES_TABLE = "bumawiki_docs_nodes"
    _EDGES_TABLE = "bumawiki_docs_edges"

    def __init__(self):
        self._catalog = create_catalog()
        self._namespace = GlueConfig.GOLD_DATABASE
        self._warehouse = GlueConfig.WAREHOUSE or f"s3://{S3Config.BUCKET_NAME}/iceberg"

    def _get_or_create_table(self, table_name: str, schema: Schema):
        table_id = (self._namespace, table_name)
        try:
            return self._catalog.load_table(table_id)
        except NoSuchTableError:
            try:
                self._catalog.create_namespace(self._namespace, {"location": self._warehouse})
            except Exception:
                pass

            ds_field_id = schema.find_field("ds").field_id
            partition_spec = PartitionSpec(
                PartitionField(source_id=ds_field_id, field_id=1000, transform=IdentityTransform(), name="ds")
            )
            location = f"{self._warehouse}/{self._namespace}/{table_name}"
            return self._catalog.create_table(
                identifier=table_id,
                schema=schema,
                partition_spec=partition_spec,
                location=location,
            )

    def save_nodes(self, nodes: List[Node], ds: str) -> None:
        rows = [
            {
                "id": n.id,
                "title": n.title,
                "type": n.type.value,
                "docs_type": n.docs_type.value,
                "last_modified_at": str(n.last_modified_at),
                "enroll": n.enroll,
                "ds": ds,
            }
            for n in nodes
        ]
        arrow_table = pl.DataFrame(rows).to_arrow()
        table = self._get_or_create_table(self._NODES_TABLE, _NODES_SCHEMA)
        table.append(arrow_table)

    def save_edges(self, edges: List[Edge], ds: str) -> None:
        rows = [
            {
                "type": e.type.value,
                "source_id": e.source.id,
                "source_title": e.source.title,
                "target_id": e.target.id,
                "target_title": e.target.title,
                "ds": ds,
            }
            for e in edges
        ]
        arrow_table = pl.DataFrame(rows).to_arrow()
        table = self._get_or_create_table(self._EDGES_TABLE, _EDGES_SCHEMA)
        table.append(arrow_table)

    def get_nodes(self, ds: str) -> pl.DataFrame:
        table = self._catalog.load_table((self._namespace, self._NODES_TABLE))
        return pl.from_arrow(table.scan(row_filter=EqualTo("ds", ds)).to_arrow())

    def get_edges(self, ds: str) -> pl.DataFrame:
        table = self._catalog.load_table((self._namespace, self._EDGES_TABLE))
        return pl.from_arrow(table.scan(row_filter=EqualTo("ds", ds)).to_arrow())
