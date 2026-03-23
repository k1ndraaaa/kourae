from native.LogManager.MainClass import LogManager, LOG_ERROR, LOG_INFO, LOG_WARN
from native.JwtManager.MainClass import JwtManager
from native.Auth.MainClass import Auth
from native.Streaming.MainClass import Streaming
from native.Library.commons import Session

from pathlib import Path

from adapters.Postgresql.MainClass import PostgresClient as SqlClient, Table
from adapters.Redis.MainClass import RedisClient
from adapters.Minio.MainClass import MinioClient

from adapters.EnvLoader.MainClass import EnvLoader, root_path

class VM:
    def __init__(self):
        self.log_manager = LogManager()
        self.jwt_manager = JwtManager()
        self.session = Session()
        mypath = Path(root_path)
        self.env = EnvLoader().load_vars_from_env(
            path= Path(mypath / ".env")
        )
        try:
            self.auth_sql_client = SqlClient(
                host= str(self.env["auth_sqlserver_host"]),
                port= int(self.env["auth_sqlserver_port"]),
                user= str(self.env["auth_sqlserver_user"]),
                password=str(self.env["auth_sqlserver_password"]),
                database=str(self.env["auth_sqlserver_database"])
            )
            self.files_sql_client = SqlClient(
                host= str(self.env["file_sqlserver_host"]),
                port= int(self.env["file_sqlserver_port"]),
                user= str(self.env["file_sqlserver_user"]),
                password=str(self.env["file_sqlserver_password"]),
                database=str(self.env["file_sqlserver_database"])
            )
            self.log_manager.log(
                level=LOG_INFO,
                message="PostgreSQL conectado correctamente",
                session=self.session,
                printq=True
            )
        except Exception as e:
            self.auth_sql_client = None
            self.files_sql_client = None
            raise Exception(f"Alguno de los clientes PostgreSQL no arranca: {e}")
        try:
            self.redis_client = RedisClient(
                host=str(self.env["redisserver_host"]),
                port=int(self.env["redisserver_port"]),
                db=int(self.env["redisserver_database"])
            )
            self.log_manager.log(
                level=LOG_INFO,
                message="Redis conectado correctamente",
                session=self.session,
                printq=True
            )
        except Exception as e:
            self.redis_client = None
            raise Exception(f"El cliente Redis no arranca: {e}")
        try:
            self.minio_client = MinioClient(
                host=str(self.env["minioserver_host"]),
                port=int(self.env["minioserver_port"]),
                user=str(self.env["minioserver_user"]),
                password=str(self.env["minioserver_password"])
            )
            self.log_manager.log(
                level=LOG_INFO,
                message="Minio conectado correctamente",
                session=self.session,
                printq=True
            )
        except Exception as e:
            self.minio_client = None
            raise Exception(f"El cliente Minio no arranca: {e}")
        if any(client is None for client in (
            self.auth_sql_client,
            self.files_sql_client,
            self.redis_client,
            self.minio_client
        )):
            raise Exception("Algunos adaptadores no arrancan")
        self.users_table = Table(
            sql_client=self.auth_sql_client,
            table_name="kouraenet_users",
            columns={
                "id": "SERIAL PRIMARY KEY",
                "username": "VARCHAR(25)",
                "password": "VARCHAR(30)"
            }
        )
        self.auth_sql_client.create_table(
            table=self.users_table,
            if_not_exists=True
        )
        self.files_table = Table(
            sql_client=self.files_sql_client,
            table_name="kouraenet_files",
            columns={
                "id": "SERIAL PRIMARY KEY",
                "user_id": "INTEGER",
                "filename": "TEXT",
                "privacy": "TEXT DEFAULT 'private'",
                "ext": "TEXT",
                "mime_type": "TEXT",
                "size": "INTEGER",
                "bucket": "TEXT",
                "object_key": "TEXT"
            }
        )
        self.files_sql_client.create_table(
            table=self.files_table,
            if_not_exists=True
        )
        self.revoked_table = Table(
            sql_client=self.auth_sql_client,
            table_name="kouraenet_revokedtokens",
            columns={
                "jti": "TEXT PRIMARY KEY",
                "revoked_at": "TIMESTAMP WITH TIME ZONE DEFAULT now()",
                "expires_at": "TIMESTAMP WITH TIME ZONE NOT NULL"
            }
        )
        self.auth_sql_client.create_table(
            table=self.revoked_table,
            if_not_exists=True
        )
        self.auth_manager = Auth(
            auth_sql_client=self.auth_sql_client,
            users_table=self.users_table,
            revoked_table= self.revoked_table,
            jwt_manager=self.jwt_manager,
            redis_client=self.redis_client
        )
        self.streaming = Streaming(
            auth_sql_client=self.auth_sql_client,
            files_sql_client=self.files_sql_client,
            users_table=self.users_table,
            files_table=self.files_table,
            minio_client=self.minio_client,
            redis_client=self.redis_client
        )