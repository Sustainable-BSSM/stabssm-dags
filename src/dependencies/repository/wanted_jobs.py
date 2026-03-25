from src.core.repository.wanted.jobs import WantedJobsRepository
from src.infra.repository.wanted.jobs import IcebergWantedJobsRepository


def get_wanted_jobs_repository() -> WantedJobsRepository:
    return IcebergWantedJobsRepository()
