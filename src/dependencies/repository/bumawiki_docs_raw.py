from src.core.repository.bumawiki.docs_raw import BumaWikiDocsRawRepository
from src.infra.repository.bumawiki.docs_raw import DuckDBBumaWikiDocsRawRepository


def get_bumawiki_docs_raw_repository() -> BumaWikiDocsRawRepository:
    return DuckDBBumaWikiDocsRawRepository()
