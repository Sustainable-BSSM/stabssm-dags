from src.core.repository.bumawiki.docs import BumaWikiDocsRepository
from src.infra.repository.bumawiki.docs import DuckDBBumaWikiDocsRepository


def get_bumawiki_docs_repository() -> BumaWikiDocsRepository:
    return DuckDBBumaWikiDocsRepository()
