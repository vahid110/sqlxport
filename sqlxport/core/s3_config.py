# sqlxport/core/s3_config.py

from dataclasses import dataclass
from typing import Optional

@dataclass
class S3Config:
    bucket: str
    key: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    endpoint_url: Optional[str] = None
    region_name: str = "us-east-1"
