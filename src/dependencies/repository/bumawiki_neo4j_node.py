import neo4j

from src.core.repository.bumawiki.neo4j_node import BumaWikiNeo4jNodeRepository
from src.infra.repository.bumawiki.neo4j_node import Neo4jBumaWikiNodeRepository


def get_bumawiki_neo4j_node_repository(driver: neo4j.Driver) -> BumaWikiNeo4jNodeRepository:
    return Neo4jBumaWikiNodeRepository(driver)
