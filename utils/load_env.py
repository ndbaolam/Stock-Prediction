import os
from dotenv import load_dotenv

load_dotenv()

class Env():
  MINIO_ROOT_USER: str = os.getenv("MINIO_ROOT_USER")
  MINIO_ROOT_PASSWORD: str = os.getenv("MINIO_ROOT_PASSWORD")
  MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT")

env = Env()