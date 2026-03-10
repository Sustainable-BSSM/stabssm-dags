import os

from dotenv import load_dotenv

load_dotenv()


class Neo4jConfig:
    URI: str = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    USER: str = os.getenv("NEO4J_USER", "neo4j")
    PASSWORD: str = os.getenv("NEO4J_PASSWORD")
