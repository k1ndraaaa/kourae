from adapters.Postgresql.MainClass import PostgresClient as SqlClient, Table
from adapters.Redis.MainClass import RedisClient
from adapters.Minio.MainClass import MinioClient
from native.Library.commons import FileMeta, StoragePointer, StorageObject, Condition
from native.Streaming.Errors import MediaStreamingError
from native.Economizador.MainClass import Economizador
from dataclasses import asdict

class Streaming:
    def __init__(
        self,
        auth_sql_client: SqlClient,
        files_sql_client: SqlClient,
        users_table: Table,
        files_table: Table,
        minio_client: MinioClient,
        redis_client: RedisClient
    ):
        self.users_sql = auth_sql_client
        self.files_sql = files_sql_client
        self.users_table = users_table
        self.files_table = files_table
        self.minio = minio_client
        self.economizador = Economizador(
            redis_client=redis_client,
            sql_client=self.files_sql
        )
    def _generate(self, response):
        try:
            while True:
                chunk = response.read(32 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            try:
                response.close()
            finally:
                if hasattr(response, "release_conn"):
                    response.release_conn()
    def upload_files(
        self,
        user_id: int,
        file_meta: FileMeta,
        storage_object: StorageObject
    ):
        try:
            self.minio.put_object(storage_object)
            try:
                query = (
                    self.files_table.insert()
                    .values(
                        user_id=user_id,
                        **asdict(file_meta)
                    )
                )
                self.files_sql.query(query)
            except Exception:
                self.minio.remove_object(
                    StoragePointer(
                        storage_object.bucket,
                        storage_object.object_key
                    )
                )
                raise
        except Exception as e:
            raise MediaStreamingError(e)
    def delete_files(self, user_id: int, file_id: int):
        query = (
            self.files_table.select()
            .columns("id", "bucket", "object_key")
            .where(Condition(self.files_table.id, "=", file_id))
            .where(Condition(self.files_table.user_id, "=", user_id))
            .limit(1)
        )
        row = self.economizador.fetchone(query)
        if not row:
            return False
        _, bucket, object_key = row
        try:
            self.minio.remove_object(
                StoragePointer(bucket, object_key)
            )
            delete_query = (
                self.files_table.delete()
                .where(Condition(self.files_table.id, "=", file_id))
                .where(Condition(self.files_table.user_id, "=", user_id))
            )
            self.economizador.delete(delete_query)
            return True
        except Exception as e:
            raise MediaStreamingError(e)
    def open(self, user_id: int, file_id: int):
        query = (
            self.files_table.select()
            .columns(
                "mime_type",
                "size",
                "bucket",
                "object_key"
            )
            .where(Condition(self.files_table.id, "=", file_id))
            .where(Condition(self.files_table.user_id, "=", user_id))
            .limit(1)
        )
        row = self.economizador.fetchone(query)
        if not row:
            return None
        mime_type, size, bucket, object_key = row
        stream = self.minio.get_object(
            StoragePointer(bucket, object_key)
        )
        return {
            "mime_type": mime_type,
            "size": size,
            "stream": self._generate(stream)
        }
    def update_files(self, user_id: int, file_id: int, filename: str):
        query = (
            self.files_table.update()
            .set(filename=filename)
            .where(Condition(self.files_table.id, "=", file_id))
            .where(Condition(self.files_table.user_id, "=", user_id))
        )
        self.economizador.update(query)
    def list_files(
        self,
        user_id: int,
        limit: int = 20,
        offset: int = 0,
        privacy: str = "public"
    ):
        if privacy not in ("public", "private"):
            raise ValueError("La opción de privacidad recibida no es válida.")
        query = (
            self.files_table.select()
            .columns(
                "id",
                "filename",
                "mime_type",
                "size",
                "privacy"
            )
            .where(Condition(self.files_table.user_id, "=", user_id))
            .where(Condition(self.files_table.privacy, "=", privacy))
            .limit(limit)
            .offset(offset)
        )
        return self.economizador.fetchall(query)
    def search_files(self, user_id: int, query_text: str, privacy: str = "public"):
        if privacy not in ("public", "private"):
            raise ValueError("La opción de privacidad recibida no es válida.")
        query = (
            self.files_table.select()
            .columns(
                "id",
                "filename",
                "mime_type",
                "privacy"
            )
            .where(Condition(self.files_table.user_id, "=", user_id))
            .where(Condition(self.files_table.filename, "ILIKE", f"%{query_text}%"))
            .where(Condition(self.files_table.privacy, "=", privacy))
        )
        return self.economizador.fetchall(query)
    def file_exists(self, user_id: int, file_id: int):
        query = (
            self.files_table.select()
            .columns("id")
            .where(Condition(self.files_table.id, "=", file_id))
            .where(Condition(self.files_table.user_id, "=", user_id))
            .limit(1)
        )
        return bool(self.economizador.scalar(query))