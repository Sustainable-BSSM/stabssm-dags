import neo4j
import polars as pl

from src.core.repository.bumawiki.neo4j_edge import BumaWikiNeo4jEdgeRepository


class Neo4jBumaWikiEdgeRepository(BumaWikiNeo4jEdgeRepository):

    def __init__(self, driver: neo4j.Driver):
        self._driver = driver

    def save(self, df: pl.DataFrame, ds: str) -> None:
        with self._driver.session() as session:
            for edge_type in df["type"].unique().to_list():
                rel_type = edge_type.upper()
                group = df.filter(pl.col("type") == edge_type).to_dicts()
                session.run(
                    f"""
                    UNWIND $edges AS e
                    MATCH (s {{id: e.source_id, ds: $ds}})
                    MATCH (t {{id: e.target_id, ds: $ds}})
                    MERGE (s)-[:{rel_type} {{ds: $ds}}]->(t)
                    """,
                    edges=group,
                    ds=ds,
                )
