import neo4j
from neo4j import GraphDatabase

from src.common.config.neo4j import Neo4jConfig


def create_driver() -> neo4j.Driver:
    return GraphDatabase.driver(
        Neo4jConfig.URI,
        auth=(Neo4jConfig.USER, Neo4jConfig.PASSWORD),
    )
