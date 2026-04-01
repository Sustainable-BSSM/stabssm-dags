from src.core.repository.wanted.jobs_silver import WantedJobsSilverRepository
from src.infra.repository.wanted.jobs_silver import IcebergWantedJobsSilverRepository


def get_wanted_jobs_silver_repository() -> WantedJobsSilverRepository:
    return IcebergWantedJobsSilverRepository()
