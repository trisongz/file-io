
import asyncio
import boto3

from httpx import AsyncClient
#from aioaws.s3 import S3Client, S3Config
from fileio.configs import CloudConfig
from fileio.cloud import auth
from fileio.cloud.base import BaseCloudFile
from fileio.static.classes import S3Config

def _auth_aws_s3(
    aws_access_key: str = CloudConfig.AWSAccessID,
    aws_secret_key: str = CloudConfig.AWSAccessKey,
    aws_region: str = CloudConfig.AWSRegion,
    aws_s3_bucket: str = CloudConfig.S3Bucket
    ):
    auth.AWS_S3_SESSION = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
    if not aws_region:
        aws_region = auth.AWS_S3_SESSION.region_name
    if not aws_access_key or aws_secret_key:
        creds = auth.AWS_S3_SESSION.get_credentials()
        aws_access_key, aws_secret_key = creds.access_key, creds.secret_key
    if aws_access_key != CloudConfig.AWSAccessID or aws_secret_key != CloudConfig.AWSAccessKey:
        CloudConfig.AWSAccessID = aws_access_key
        CloudConfig.AWSAccessKey = aws_secret_key
        CloudConfig.AWSRegion = aws_region
        CloudConfig.set_environ()
    auth.AWS_S3_AUTH = S3Config(aws_access_key, aws_secret_key, aws_region, aws_s3_bucket)
    


#class AWSS3File(BaseCloudFile):





