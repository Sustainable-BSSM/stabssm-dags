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
