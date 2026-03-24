from native.LogManager.MainClass import LogManager, LOG_ERROR, LOG_INFO, LOG_WARN
from native.JwtManager.MainClass import JwtManager
from native.Auth.MainClass import Auth
from native.Streaming.MainClass import Streaming
from native.Library.commons import Session, Router
from native.Library.translators import translate_request
from pathlib import Path
from functools import wraps
from adapters.Postgresql.MainClass import PostgresClient as SqlClient, Table
from adapters.Redis.MainClass import RedisClient
from adapters.Minio.MainClass import MinioClient
from adapters.EnvLoader.MainClass import EnvLoader, root_path
from adapters.EnvLoader.Errors import EnvLoaderError
import importlib, inspect
from flask import Flask, request
from native.Library.guards import RequestContext,set_request_context,GuardPipeline,AuthGuard,HeaderGuard,ContentTypeGuard

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

class FlaskVM:
    def __init__(
        self,
        name,
        vm: VM
    ):
        self.app = Flask(name)
        self.vm = vm
        self.root_path = Path(root_path)
        self.envloader = EnvLoader()
        self.env = self.envloader.load_vars_from_env(
            path=self.root_path / ".env"
        )
    def guarded_endpoint(
        self,
        *,
        require_auth: bool = False,
        expected_mimetype=None,
        request_headers=None,
        guards=None
    ):
        pipeline_guards = []
        if expected_mimetype:
            pipeline_guards.append(
                ContentTypeGuard(expected_mimetype)
            )
        if request_headers:
            pipeline_guards.append(
                HeaderGuard(request_headers)
            )
        if require_auth:
            pipeline_guards.append(
                AuthGuard()
            )
        if guards:
            pipeline_guards.extend(guards)
        pipeline = GuardPipeline(pipeline_guards)
        def decorator(fn):
            params = inspect.signature(fn).parameters
            wants_request = "request" in params
            @wraps(fn)
            def wrapper(*args, **kwargs):
                framework_request = request
                standar_request = translate_request(
                    framework_request
                )
                ctx_obj = RequestContext(
                    request=standar_request,
                    vm=self.vm
                )
                set_request_context(ctx_obj)
                ok, resp = pipeline.run(ctx_obj)
                if not ok:
                    return ctx_obj.vm.log_manager.http_response(resp)
                if wants_request:
                    kwargs["request"] = ctx_obj.request
                return fn(*args, **kwargs)
            return wrapper
        return decorator
    def route(
        self,
        route: str,
        methods=None,
        require_auth=False,
        expected_mimetype=None,
        request_headers=None,
        guards=None
    ):
        methods = methods or ["GET"]
        route = route.replace("{", "<").replace("}", ">")
        def decorator(func):
            func = self.guarded_endpoint(
                require_auth=require_auth,
                expected_mimetype=expected_mimetype,
                request_headers=request_headers,
                guards=guards
            )(func)
            return self.app.route(
                route,
                methods=methods
            )(func)
        return decorator
    def get(self, route, **kwargs):
        return self.route(route, methods=["GET"], **kwargs)
    def post(self, route, **kwargs):
        return self.route(route, methods=["POST"], **kwargs)
    def put(self, route, **kwargs):
        return self.route(route, methods=["PUT"], **kwargs)
    def delete(self, route, **kwargs):
        return self.route(route, methods=["DELETE"], **kwargs)
    def group(self, prefix: str):
        return Router(self, prefix)
    def mapApplications(self):
        microapps = self.envloader.scan_directory(
            directory=self.root_path / "microapps",
            root_path=self.root_path
        )
        for _, meta in microapps.items():
            if meta["type"] != "dir":
                continue
            source = Path(meta["source"])
            endpoints_path = source / "web" / "Endpoints.py"
            if not endpoints_path.exists():
                continue
            pypath = self.envloader.path_to_pypath(
                endpoints_path,
                self.root_path
            )
            url_prefix = source.name
            self._mapApplication(
                pypath=pypath,
                url_prefix=url_prefix
            )
    def _mapApplication(
        self,
        *,
        pypath: str,
        url_prefix: str
    ):
        try:
            module = importlib.import_module(pypath)
        except Exception as e:
            raise EnvLoaderError(
                f"Error importando '{pypath}': {e}"
            )
        if not hasattr(module, "main"):
            raise EnvLoaderError(
                f"El módulo '{pypath}' debe exponer main(vm)"
            )
        module.main(flask_vm=self)
        print(
            f"Registrado: /{url_prefix} ← {pypath}"
        )
    def play_and_debug(self):
        self.mapApplications()
        host = str(self.env.get("flask_host"))
        port = int(self.env.get("flask_port"))
        self.app.run(
            host=host,
            port=port,
            debug=True
        )