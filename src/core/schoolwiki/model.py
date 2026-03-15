from dataclasses import dataclass
from enum import Enum

from src.core.bumawiki.model import BumaWikiDocsType


class SchoolwikiCategory(str, Enum):
    STUDENT = "STUDENT"
    TEACHER = "TEACHER"
    INCIDENT = "INCIDENT"
    CLUB = "CLUB"

    @property
    def url_slug(self) -> str:
        """RSC URL에서 사용하는 frontendCategory 값."""
        return self.value.lower()

    def to_bumawiki_docs_type(self) -> BumaWikiDocsType:
        mapping = {
            SchoolwikiCategory.STUDENT: BumaWikiDocsType.STUDENT,
            SchoolwikiCategory.TEACHER: BumaWikiDocsType.TEACHER,
            SchoolwikiCategory.INCIDENT: BumaWikiDocsType.ACCIDENT,
            SchoolwikiCategory.CLUB: BumaWikiDocsType.CLUB,
        }
        return mapping[self]


@dataclass
class SchoolwikiDocument:
    id: str
    title: str
    slug: str
    category: str
    summary: str | None
    toggle_id: str | None
    year: int | None
    updated_at: str | None

    @classmethod
    def from_dict(cls, data: dict) -> "SchoolwikiDocument":
        return cls(
            id=data.get("id", ""),
            title=data.get("title", ""),
            slug=data.get("slug", ""),
            category=data.get("category", ""),
            summary=data.get("summary"),
            toggle_id=data.get("toggleId"),
            year=data.get("year"),
            updated_at=data.get("updatedAt"),
        )
