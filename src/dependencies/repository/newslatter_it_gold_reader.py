from src.infra.repository.newslatter.news_gold_reader import IcebergNewsGoldReader


def get_it_gold_reader() -> IcebergNewsGoldReader:
    return IcebergNewsGoldReader(table_name="newslatter_it")
