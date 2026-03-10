import neo4j
import polars as pl

from src.core.repository.bumawiki.neo4j_node import BumaWikiNeo4jNodeRepository


class Neo4jBumaWikiNodeRepository(BumaWikiNeo4jNodeRepository):

    def __init__(self, driver: neo4j.Driver):
        self._driver = driver

    def _ensure_database(self, db_name: str) -> None:
        with self._driver.session(database="system") as session:
            session.run(f"CREATE DATABASE `{db_name}` IF NOT EXISTS")

    def save(self, df: pl.DataFrame, db_name: str) -> None:
        self._ensure_database(db_name)

        with self._driver.session(database=db_name) as session:
            for type_value in df["type"].unique().to_list():
                label = type_value.capitalize()
                group = df.filter(pl.col("type") == type_value).to_dicts()
                session.run(
                    f"""
                    UNWIND $nodes AS n
                    MERGE (node:{label} {{id: n.id}})
                    SET node.title = n.title,
                        node.docs_type = n.docs_type,
                        node.last_modified_at = n.last_modified_at,
                        node.enroll = n.enroll
                    """,
                    nodes=group,
                )
