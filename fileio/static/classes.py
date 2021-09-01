from dataclasses import dataclass

@dataclass
class GCSConfig:
    gcp_adc: str
    gcp_project: str
    gcp_zone: str


@dataclass
class SupabaseConfig:
    supbase_url: str
    supabase_key: str



@dataclass
class S3Config:
    aws_access_key: str
    aws_secret_key: str
    aws_region: str
    aws_s3_bucket: str