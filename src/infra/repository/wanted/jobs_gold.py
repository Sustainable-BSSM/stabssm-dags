import logging
import random
import time

import polars as pl
import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError, TableAlreadyExistsError
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table.name_mapping import MappedField, NameMapping
from pyiceberg.transforms import IdentityTransform

from src.common.config.glue import GlueConfig
from src.common.config.s3 import S3Config
from src.core.repository.wanted.jobs_gold import WantedJobsGoldRepository
from src.infra.iceberg import create_catalog

logger = logging.getLogger(__name__)


class IcebergWantedJobsGoldRepository(WantedJobsGoldRepository):

    def __init__(self, table_name: str = "wanted_jobs_tiered"):
        self._TABLE_NAME = table_name
        self._catalog = create_catalog()
        self._namespace = GlueConfig.GOLD_DATABASE
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
            dt_field_id = iceberg_schema.find_field("dt").field_id
            partition_spec = PartitionSpec(
                PartitionField(source_id=dt_field_id, field_id=1000, transform=IdentityTransform(), name="dt"),
            )
            location = f"{self._warehouse}/{self._namespace}/{self._TABLE_NAME}"
            try:
                return self._catalog.create_table(
                    identifier=self._table_id,
                    schema=iceberg_schema,
                    partition_spec=partition_spec,
                    location=location,
                )
            except TableAlreadyExistsError:
                return self._catalog.load_table(self._table_id)

    def read_top(self, ds: str, n: int = 5) -> pl.DataFrame:
        try:
            table = self._catalog.load_table(self._table_id)
            df = pl.from_arrow(table.scan(row_filter=EqualTo("dt", ds)).to_arrow())
            if df.is_empty():
                return df
            tier_order = {"S": 0, "A+": 1, "A": 2, "B": 3, "C": 4}
            df = (
                df
                .filter(pl.col("tier") != "C")
                .with_columns(pl.col("tier").replace(tier_order).alias("_tier_order"))
                .sort(["_tier_order", "score"], descending=[False, True])
                .drop("_tier_order")
            )
            logger.info(f"[IcebergWantedJobsGoldRepository] read_top {n} rows (ds={ds})")
            return df.head(n)
        except NoSuchTableError:
            logger.warning(f"[IcebergWantedJobsGoldRepository] table not found")
            return pl.DataFrame()
        except Exception as e:
            logger.error(f"[IcebergWantedJobsGoldRepository] read_top failed: {e}")
            return pl.DataFrame()

    def save(self, df: pl.DataFrame, ds: str) -> None:
        arrow_table = df.to_arrow()
        for attempt in range(10):
            try:
                table = self._get_or_create_table(arrow_table)
                table.overwrite(arrow_table, overwrite_filter=EqualTo("dt", ds))
                logger.info(f"[IcebergWantedJobsGoldRepository] saved {len(df)} rows (ds={ds})")
                return
            except CommitFailedException as e:
                if attempt == 9:
                    raise
                wait = min(2 ** attempt + random.random(), 60.0)
                logger.warning(f"[IcebergWantedJobsGoldRepository] commit conflict, retry {attempt + 1}/10 after {wait:.1f}s: {e}")
                time.sleep(wait)
