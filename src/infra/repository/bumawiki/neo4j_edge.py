import neo4j
import polars as pl

from src.core.repository.bumawiki.neo4j_edge import BumaWikiNeo4jEdgeRepository


class Neo4jBumaWikiEdgeRepository(BumaWikiNeo4jEdgeRepository):

    def __init__(self, driver: neo4j.Driver):
        self._driver = driver

    def save(self, df: pl.DataFrame, db_name: str) -> None:
        with self._driver.session(database=db_name) as session:
            for edge_type in df["type"].unique().to_list():
                rel_type = edge_type.upper()
                group = df.filter(pl.col("type") == edge_type).to_dicts()
                session.run(
                    f"""
                    UNWIND $edges AS e
                    MATCH (s {{id: e.source_id}})
                    MATCH (t {{id: e.target_id}})
                    MERGE (s)-[:{rel_type}]->(t)
                    """,
                    edges=group,
                )
