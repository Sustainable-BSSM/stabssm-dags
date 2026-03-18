from src.core.repository.newslatter.news import NewsRepository
from src.infra.repository.newslatter.news import IcebergNewsRepository


def get_it_news_repository() -> NewsRepository:
    return IcebergNewsRepository(table_name="newslatter_it")
