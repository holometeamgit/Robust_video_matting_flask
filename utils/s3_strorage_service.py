import io
import logging
import os
from secrets import token_hex

import boto3
from django.conf import settings


class StorageFile:
    def __init__(self, s3_key: str, file_name: str, bucket: str, size: int):
        self.s3_key = s3_key
        self.file_name = file_name
        self.size = size
        self.bucket = bucket


class S3StorageService:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3 = boto3.resource(
            's3',
            endpoint_url=settings.AWS_S3_ENDPOINT_URL,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
        self.client = boto3.client(
            's3',
            endpoint_url=settings.AWS_S3_ENDPOINT_URL,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
        self.bucket = self.s3.Bucket(bucket_name)

    def upload(self, file_obj, s3_key=None, file_name=None, public=False) -> StorageFile:
        if s3_key is None:
            s3_key = f'{token_hex(32)}___{file_name.replace(" ", "")}'

        acl_policy = {}
        if public:
            acl_policy['ACL'] = 'public-read'

        mime = self._check_meme_type(file_name)
        if mime:
            acl_policy['ContentType'] = mime

        self.bucket.upload_fileobj(file_obj, s3_key, ExtraArgs=acl_policy)
        storage_file = StorageFile(s3_key, file_name, self.bucket_name, self._check_size(file_obj))

        return storage_file

    def _check_meme_type(self, filename):
        extension = os.path.splitext(filename)[1].lower()
        if extension == '.mp4':
            return 'video/mp4'
        if extension == '.jpg' or extension == '.jpeg':
            return 'image/jpeg'
        if extension == '.png':
            return 'image/png'
        if extension == '.gif':
            return 'image/gif'

    def _check_size(self, file_obj):
        if hasattr(file_obj, 'size'):
            return file_obj.size
        if hasattr(file_obj, '__len__'):
            return len(file_obj)
        return -1

    def delete(self, file_key: str) -> None:
        self.s3.Object(settings.S3_BUCKET_NAME, file_key).delete()

    def upload_public_read(self, file_obj, file_name: str, s3_key=None) -> StorageFile:
        return self.upload(file_obj, s3_key, file_name, public=True)

    def upload_stream(self, stream: io.BytesIO, s3_key: str = None, file_name: str = None, public: bool = False):
        with io.BytesIO(stream.getvalue()) as s:
            self.upload(s, s3_key, file_name, public)

    def download(self, storage_file: StorageFile, stream: io.BytesIO):
        logging.info(f'download from s3: {storage_file.s3_key}  -->  io stream')
        self.bucket.download_fileobj(storage_file.s3_key, stream)

    def download_file(self, s3_key: str, file_name: str):
        logging.info(f'download from s3: {s3_key}  -->  {file_name}')
        self.bucket.download_file(s3_key, file_name)

