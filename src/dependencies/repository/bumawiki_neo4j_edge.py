import neo4j

from src.core.repository.bumawiki.neo4j_edge import BumaWikiNeo4jEdgeRepository
from src.infra.repository.bumawiki.neo4j_edge import Neo4jBumaWikiEdgeRepository


def get_bumawiki_neo4j_edge_repository(driver: neo4j.Driver) -> BumaWikiNeo4jEdgeRepository:
    return Neo4jBumaWikiEdgeRepository(driver)
