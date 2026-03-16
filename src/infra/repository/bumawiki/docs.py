import logging

import polars as pl
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table.name_mapping import MappedField, NameMapping
from pyiceberg.transforms import IdentityTransform

from src.common.config.glue import GlueConfig
from src.common.config.s3 import S3Config
from src.core.repository.bumawiki.docs import BumaWikiDocsRepository
from src.infra.iceberg import create_catalog

logger = logging.getLogger(__name__)


class IcebergBumaWikiDocsRepository(BumaWikiDocsRepository):

    _TABLE_NAME = "bumawiki_docs"

    def __init__(self):
        self._catalog = create_catalog()
        self._namespace = GlueConfig.SILVER_DATABASE
        self._warehouse = GlueConfig.WAREHOUSE or f"s3://{S3Config.BUCKET_NAME}/iceberg"
        self._table_id = (self._namespace, self._TABLE_NAME)

    def _get_or_create_table(self, arrow_table: pa.Table):
        try:
            return self._catalog.load_table(self._table_id)
        except NoSuchTableError:
            try:
                self._catalog.create_namespace(self._namespace, {"location": self._warehouse})
            except Exception:
                pass

            name_mapping = NameMapping([
                MappedField(field_id=i + 1, names=[field.name])
                for i, field in enumerate(arrow_table.schema)
            ])
            iceberg_schema = pyarrow_to_schema(arrow_table.schema, name_mapping=name_mapping)
            ds_field_id = iceberg_schema.find_field("ds").field_id
            partition_spec = PartitionSpec(
                PartitionField(source_id=ds_field_id, field_id=1000, transform=IdentityTransform(), name="ds")
            )
            location = f"{self._warehouse}/{self._namespace}/{self._TABLE_NAME}"
            return self._catalog.create_table(
                identifier=self._table_id,
                schema=iceberg_schema,
                partition_spec=partition_spec,
                location=location,
            )

    def save(self, df: pl.DataFrame, ds: str) -> None:
        arrow_table = df.with_columns(pl.lit(ds).alias("ds")).to_arrow()
        table = self._get_or_create_table(arrow_table)
        table.append(arrow_table)
        logger.info(f"[DocsRepository] saved {len(df)} rows to iceberg (ds={ds})")

    async def get(self, ds: str) -> pl.DataFrame:
        try:
            table = self._catalog.load_table(self._table_id)
            scan = table.scan(row_filter=EqualTo("ds", ds))
            df = pl.from_arrow(scan.to_arrow())
            logger.info(f"[DocsRepository] loaded {len(df)} rows from iceberg (ds={ds})")
            return df
        except Exception as e:
            logger.error(f"[DocsRepository] failed to read iceberg: {e}")
            return pl.DataFrame()
