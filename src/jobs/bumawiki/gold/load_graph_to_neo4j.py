import argparse

import neo4j

from src.core.jobs import Job
from src.core.repository.bumawiki.graph import BumaWikiGraphRepository
from src.core.repository.bumawiki.neo4j_edge import BumaWikiNeo4jEdgeRepository
from src.core.repository.bumawiki.neo4j_node import BumaWikiNeo4jNodeRepository
from src.dependencies.repository.bumawiki_graph import get_bumawiki_graph_repository
from src.dependencies.repository.bumawiki_neo4j_edge import get_bumawiki_neo4j_edge_repository
from src.dependencies.repository.bumawiki_neo4j_node import get_bumawiki_neo4j_node_repository
from src.infra.neo4j import create_driver


class LoadGraphToNeo4jJob(Job):

    def __init__(
            self,
            graph_repo: BumaWikiGraphRepository,
            node_repo: BumaWikiNeo4jNodeRepository,
            edge_repo: BumaWikiNeo4jEdgeRepository,
    ):
        self._graph_repo = graph_repo
        self._node_repo = node_repo
        self._edge_repo = edge_repo

    def __call__(self, ds: str):
        db_name = f"bumawiki-{ds}"

        nodes_df = self._graph_repo.get_nodes(ds)
        edges_df = self._graph_repo.get_edges(ds)

        self._node_repo.save(
            df=nodes_df,
            db_name=db_name
        )
        self._edge_repo.save(
            df=edges_df,
            db_name=db_name
        )


def run_job(ds: str):
    driver: neo4j.Driver = create_driver()
    try:
        graph_repo = get_bumawiki_graph_repository()
        node_repo = get_bumawiki_neo4j_node_repository(driver)
        edge_repo = get_bumawiki_neo4j_edge_repository(driver)
        job = LoadGraphToNeo4jJob(
            graph_repo=graph_repo,
            node_repo=node_repo,
            edge_repo=edge_repo,
        )
        job(ds=ds)
    finally:
        driver.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
