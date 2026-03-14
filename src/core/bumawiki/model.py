from dataclasses import dataclass
from enum import Enum


class BumaWikiDocsType(str, Enum):
    STUDENT = 'student'
    TEACHER = 'teacher'
    ACCIDENT = 'accident'
    CLUB = 'club'

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str) and value.endswith('_teacher'):
            return cls.TEACHER
        return None


class TeacherType(str, Enum):
    GENERAL = "teacher"
    MAJOR = "major_teacher"
    MENTOR = "mentor_teacher"
    # 보통 교과 = teacher
    # 전공 교과 = major_teacher
    # 멘토 선생님 = mentor_teacher


@dataclass
class Contributor:
    id: int
    email: str
    nick_name: str
    name: str

    @classmethod
    def from_dict(cls, d: dict) -> "Contributor":
        return cls(id=d["id"], email=d["email"], nick_name=d["nickName"], name=d["name"])
