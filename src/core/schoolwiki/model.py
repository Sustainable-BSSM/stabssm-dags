from dataclasses import dataclass
from enum import Enum


class SchoolwikiCategory(str, Enum):
    STUDENT = "STUDENT"
    TEACHER = "TEACHER"
    INCIDENT = "INCIDENT"
    CLUB = "CLUB"

    @property
    def url_slug(self) -> str:
        """RSC URL에서 사용하는 frontendCategory 값."""
        return self.value.lower()


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
