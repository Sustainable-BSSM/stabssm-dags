from src.core.repository.newslatter.news import NewsRepository
from src.infra.repository.newslatter.news import IcebergNewsRepository


def get_news_repository() -> NewsRepository:
    return IcebergNewsRepository()
