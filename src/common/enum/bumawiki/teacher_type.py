from enum import Enum


class TeacherType(str, Enum):
    GENERAL = "teacher"
    MAJOR = "major_teacher"
    MENTOR = "mentor_teacher"
    # 보통 교과 = teacher
    # 전공 교과 = major_teacher
    # 멘토 선생님 = mentor_teacher
