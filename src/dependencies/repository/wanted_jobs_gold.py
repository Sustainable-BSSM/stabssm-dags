from src.core.repository.wanted.jobs_gold import WantedJobsGoldRepository
from src.infra.repository.wanted.jobs_gold import IcebergWantedJobsGoldRepository


def get_wanted_jobs_gold_repository() -> WantedJobsGoldRepository:
    return IcebergWantedJobsGoldRepository()
