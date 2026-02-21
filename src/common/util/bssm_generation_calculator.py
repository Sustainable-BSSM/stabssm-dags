from src.common.const.year_established import FIRST_BSSM_YEAR


class BSSMGenerationCalculator:

    @staticmethod
    def calculate(year : int) -> int:
        """
        부소마고 기수 계산기
        2023년 -> 3기
        2024년 -> 4기
        2025년 -> 5기
        2026년 -> 6기
        ·······
        2100년 -> 80기
        """
        return (int(year) - FIRST_BSSM_YEAR) + 1