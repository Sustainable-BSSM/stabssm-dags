from enum import Enum


class BumaWikiDocsType(str, Enum):
    STUDENT = 'student'
    TEACHER = 'teacher'
    ACCIDENT = 'accident'
    CLUB = 'club'