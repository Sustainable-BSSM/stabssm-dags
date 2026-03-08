import os
from dotenv import load_dotenv
load_dotenv()

class OpenAIConfig:
    API_KEY : str = os.getenv("OPENAI_API_KEY")

    @classmethod
    def to_env_dict(cls) -> dict:
        return {
            "OPENAPI_API_KEY": cls.API_KEY,
        }