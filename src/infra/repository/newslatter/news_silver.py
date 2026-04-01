import logging

import polars as pl
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo

from src.common.config.glue import GlueConfig
from src.core.repository.newslatter.news_silver import NewsSilverRepository
from src.infra.iceberg import create_catalog

logger = logging.getLogger(__name__)


class IcebergNewsSilverRepository(NewsSilverRepository):

    def __init__(self, table_name: str = "newslatter_school"):
        self._TABLE_NAME = table_name
        self._catalog = create_catalog()
        self._namespace = GlueConfig.SILVER_DATABASE
        self._table_id = (self._namespace, self._TABLE_NAME)

    def read(self, week: str) -> pl.DataFrame:
        try:
            table = self._catalog.load_table(self._table_id)
            df = pl.from_arrow(table.scan(row_filter=EqualTo("week", week)).to_arrow())
            logger.info(f"[IcebergNewsSilverRepository] loaded {len(df)} rows (week={week})")
            return df
        except NoSuchTableError:
            logger.warning("[IcebergNewsSilverRepository] table not found")
            return pl.DataFrame()
        except Exception as e:
            logger.error(f"[IcebergNewsSilverRepository] read failed: {e}")
            return pl.DataFrame()
