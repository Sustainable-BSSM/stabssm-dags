import os

from dotenv import load_dotenv

load_dotenv()


class GlueConfig:
    SILVER_DATABASE: str = os.getenv("GLUE_SILVER_DATABASE", "stabssm_silver")
    GOLD_DATABASE: str = os.getenv("GLUE_GOLD_DATABASE", "stabssm_gold")
    WAREHOUSE: str = os.getenv("ICEBERG_WAREHOUSE")
