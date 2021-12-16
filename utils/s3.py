import io
import boto3

from botocore.client import ClientError
from django.conf import settings

from stream.management.commands.const_bucket_list import bucket_names

s3 = boto3.resource('s3',
                    endpoint_url=settings.AWS_S3_ENDPOINT_URL,
                    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)


def create_buckets():
    for n in bucket_names:
        create_bucket_if_not_exist(n)


def create_bucket_if_not_exist(bucket_name: str):
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)


class S3Client:
    _s3_client = boto3.client(
        's3',
        endpoint_url=settings.AWS_S3_ENDPOINT_URL,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

    def __new__(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def size(cls, bucket: str, key: str):
        response = cls._s3_client.head_object(Bucket=bucket, Key=key)
        size = response['ContentLength']
        return size

    @classmethod
    def download(cls, bucket: str, key: str, stream: io.BytesIO) -> None:
        cls._s3_client.download_fileobj(bucket, key, stream)

    @classmethod
    def upload(cls, bucket: str, key: str, stream: io.BytesIO) -> None:
        with io.BytesIO(stream.getvalue()) as s:
            cls._s3_client.upload_fileobj(s, bucket, key)
