import os

from dotenv import load_dotenv

load_dotenv()


class OpenRouterConfig:
    API_KEY: str = os.getenv("OPENROUTER_API_KEY")
    BASE_URL: str = "https://openrouter.ai/api/v1"

    @classmethod
    def to_env_dict(cls) -> dict:
        return {
            "OPENROUTER_API_KEY": cls.API_KEY,
        }
