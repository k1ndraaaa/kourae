from adapters.EnvLoader.MainClass import *
from native.Library.commons import StorageObject, StoragePointer
from adapters.Minio.Errors import *

import os
from minio import Minio
from minio.error import S3Error

class MinioClient:
    def __init__(
        self,
        host:str,
        port:int,
        user:str,
        password:str
    ):
        try:
            self.client = Minio(
                f"{host}:{port}",
                access_key=user,
                secret_key=password,
                secure=False
            )
        except Exception as e:
            raise AdapterError(e) from e
        
    def healthcheck(self):
        try:
            self.client.list_buckets()
            return True
        except Exception as e:
            raise AdapterError(e) from e
    def ensure_bucket(self, bucket: str):
        try:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
        except Exception as e:
            raise AdapterError(e) from e
    def put_object(self, obj: StorageObject) -> None:
        try:
            self.ensure_bucket(obj.bucket)
            self.MinioClient.client.put_object(
                bucket_name=obj.bucket,
                object_name=obj.object_key,
                data=obj.data,
                length=obj.length,
                content_type=obj.mime_type,
            )
        except S3Error as e:
            raise MinioClientError(f"Error subiendo objeto: {e}")
    def remove_object(self, obj: StoragePointer) -> None:
        try:
            self.MinioClient.client.remove_object(
                bucket_name=obj.bucket,
                object_name=obj.object_key
            )
        except S3Error as e:
            raise MinioClientError(f"Error eliminando objeto: {e}")