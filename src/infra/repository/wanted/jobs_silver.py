import logging

import polars as pl
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo

from src.common.config.glue import GlueConfig
from src.core.repository.wanted.jobs_silver import WantedJobsSilverRepository
from src.infra.iceberg import create_catalog

logger = logging.getLogger(__name__)


class IcebergWantedJobsSilverRepository(WantedJobsSilverRepository):

    def __init__(self, table_name: str = "wanted_jobs"):
        self._TABLE_NAME = table_name
        self._catalog = create_catalog()
        self._namespace = GlueConfig.SILVER_DATABASE
        self._table_id = (self._namespace, self._TABLE_NAME)

    def read(self, ds: str) -> pl.DataFrame:
        try:
            table = self._catalog.load_table(self._table_id)
            df = pl.from_arrow(table.scan(row_filter=EqualTo("dt", ds)).to_arrow())
            logger.info(f"[IcebergWantedJobsSilverRepository] loaded {len(df)} rows (ds={ds})")
            return df
        except NoSuchTableError:
            logger.warning("[IcebergWantedJobsSilverRepository] table not found")
            return pl.DataFrame()
        except Exception as e:
            logger.error(f"[IcebergWantedJobsSilverRepository] read failed: {e}")
            return pl.DataFrame()
