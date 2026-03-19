import logging

import polars as pl
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo

from src.common.config.glue import GlueConfig
from src.infra.iceberg import create_catalog

logger = logging.getLogger(__name__)


class IcebergNewsGoldReader:

    def __init__(self, table_name: str):
        self._catalog = create_catalog()
        self._namespace = GlueConfig.GOLD_DATABASE
        self._table_id = (self._namespace, table_name)

    def read_representatives(self, week: str) -> pl.DataFrame:
        try:
            table = self._catalog.load_table(self._table_id)
            df = pl.from_arrow(table.scan(row_filter=EqualTo("week", week)).to_arrow())
            df = df.filter(pl.col("is_representative"))
            logger.info(f"[IcebergNewsGoldReader] {self._table_id} loaded {len(df)} representative rows (week={week})")
            return df
        except NoSuchTableError:
            logger.warning(f"[IcebergNewsGoldReader] table {self._table_id} not found")
            return pl.DataFrame()
        except Exception as e:
            logger.error(f"[IcebergNewsGoldReader] read failed: {e}")
            return pl.DataFrame()
