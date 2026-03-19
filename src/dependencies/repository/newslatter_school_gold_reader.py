from src.infra.repository.newslatter.news_gold_reader import IcebergNewsGoldReader


def get_school_gold_reader() -> IcebergNewsGoldReader:
    return IcebergNewsGoldReader(table_name="newslatter_school")
