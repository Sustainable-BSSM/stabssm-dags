from src.core.repository.newslatter.news_silver import NewsSilverRepository
from src.infra.repository.newslatter.news_silver import IcebergNewsSilverRepository


def get_news_silver_repository() -> NewsSilverRepository:
    return IcebergNewsSilverRepository()
