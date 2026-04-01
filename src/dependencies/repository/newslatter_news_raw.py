from src.core.repository.newslatter.news_raw import NewsRawRepository
from src.infra.repository.newslatter.news_raw import DuckDBNewsRawRepository


def get_news_raw_repository() -> NewsRawRepository:
    return DuckDBNewsRawRepository()
