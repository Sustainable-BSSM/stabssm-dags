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
from src.core.repository.newslatter.news_gold import NewsGoldRepository
from src.infra.iceberg import create_catalog

logger = logging.getLogger(__name__)


class IcebergNewsGoldRepository(NewsGoldRepository):

    def __init__(self, table_name: str = "newslatter_school"):
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
            week_field_id = iceberg_schema.find_field("week").field_id
            partition_spec = PartitionSpec(
                PartitionField(source_id=week_field_id, field_id=1000, transform=IdentityTransform(), name="week")
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

    def save(self, df: pl.DataFrame) -> None:
        arrow_table = df.to_arrow()
        week = df["week"][0]
        for attempt in range(5):
            try:
                table = self._get_or_create_table(arrow_table)
                try:
                    table.delete(EqualTo("week", week))
                except CommitFailedException:
                    raise
                except Exception:
                    pass
                table = self._catalog.load_table(self._table_id)
                table.append(arrow_table)
                logger.info(f"[IcebergNewsGoldRepository] saved {len(df)} rows (week={week})")
                return
            except CommitFailedException as e:
                if attempt == 4:
                    raise
                wait = 2 ** attempt + random.random()
                logger.warning(f"[IcebergNewsGoldRepository] commit conflict, retry {attempt + 1}/5 after {wait:.1f}s: {e}")
                time.sleep(wait)
