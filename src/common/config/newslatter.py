import os

from dotenv import load_dotenv

load_dotenv()


class NaverConfig:
    CLIENT_ID: str = os.getenv("NAVER_NEWS_CLIENT_ID")
    CLIENT_SECRET: str = os.getenv("NAVER_NEWS_SECRET")

    @classmethod
    def to_env_dict(cls) -> dict:
        return {
            "NAVER_NEWS_CLIENT_ID": cls.CLIENT_ID,
            "NAVER_NEWS_SECRET": cls.CLIENT_SECRET,
        }
