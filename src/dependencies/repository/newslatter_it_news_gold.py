from src.core.repository.newslatter.news_gold import NewsGoldRepository
from src.infra.repository.newslatter.news_gold import IcebergNewsGoldRepository


def get_it_news_gold_repository() -> NewsGoldRepository:
    return IcebergNewsGoldRepository(table_name="newslatter_it")
