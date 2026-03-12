import neo4j
import polars as pl

from src.core.repository.bumawiki.neo4j_node import BumaWikiNeo4jNodeRepository


class Neo4jBumaWikiNodeRepository(BumaWikiNeo4jNodeRepository):

    def __init__(self, driver: neo4j.Driver):
        self._driver = driver

    def save(self, df: pl.DataFrame, ds: str) -> None:
        with self._driver.session() as session:
            for type_value in df["type"].unique().to_list():
                label = type_value.capitalize()
                group = df.filter(pl.col("type") == type_value).to_dicts()
                session.run(
                    f"""
                    UNWIND $nodes AS n
                    MERGE (node:{label} {{id: n.id, ds: $ds}})
                    SET node.title = n.title,
                        node.docs_type = n.docs_type,
                        node.last_modified_at = n.last_modified_at,
                        node.enroll = n.enroll
                    """,
                    nodes=group,
                    ds=ds,
                )
