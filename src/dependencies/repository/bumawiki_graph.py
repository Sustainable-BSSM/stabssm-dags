from src.core.repository.bumawiki.graph import BumaWikiGraphRepository
from src.infra.repository.bumawiki.graph import DuckDBBumaWikiGraphRepository


def get_bumawiki_graph_repository() -> BumaWikiGraphRepository:
    return DuckDBBumaWikiGraphRepository()
