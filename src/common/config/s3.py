import os
from dotenv import load_dotenv
load_dotenv()

class S3Config:
    ACCESS_KEY :str = os.getenv("S3_ACCESS_KEY")
    SECRET_KEY : str = os.getenv("S3_SECRET_KEY")
    BUCKET_NAME : str = os.getenv("S3_BUCKET_NAME")
    REGION : str = os.getenv("S3_REGION", "ap-northeast-2")