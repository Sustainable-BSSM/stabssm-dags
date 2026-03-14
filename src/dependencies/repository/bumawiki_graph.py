from src.core.repository.bumawiki.graph import BumaWikiGraphRepository
from src.infra.repository.bumawiki.graph import IcebergBumaWikiGraphRepository


def get_bumawiki_graph_repository() -> BumaWikiGraphRepository:
    return IcebergBumaWikiGraphRepository()
