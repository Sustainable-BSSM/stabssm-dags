from src.core.repository.wanted.jobs_raw import WantedJobsRawRepository
from src.infra.repository.wanted.jobs_raw import DuckDBWantedJobsRawRepository


def get_wanted_jobs_raw_repository() -> WantedJobsRawRepository:
    return DuckDBWantedJobsRawRepository()
