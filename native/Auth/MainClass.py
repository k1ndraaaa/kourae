from adapters.Postgresql.MainClass import PostgresClient as SqlClient, Table
from native.Library.commons import UserForm, UserUpdateForm, Session, Condition
import bcrypt
from uuid import uuid4
from dataclasses import asdict
from native.Economizador.MainClass import Economizador
from native.JwtManager.MainClass import JwtManager
from native.Library.commons import Request as StandarRequest
from adapters.Redis.MainClass import RedisClient
DUMMY_HASH = b"$2b$12$C6UzMDM.H6dfI/f/IKcEeO9p9jv6vFVLtZL1NVr7DiIP9N6byN1Ga"

class Auth:
    def __init__(
        self,
        auth_sql_client: SqlClient,
        users_table: Table,
        revoked_table: Table,
        jwt_manager: JwtManager,
        redis_client: RedisClient
    ):
        self.sql_client = auth_sql_client
        self.jwt_manager = jwt_manager
        self.users_table = users_table
        self.revoked_table = revoked_table
        self.economizador = Economizador(
            redis_client=redis_client,
            sql_client=self.sql_client
        )
    def _is_password_correct(self, hashed_password: bytes, password: str):
        try:
            if not hashed_password:
                hashed_password = DUMMY_HASH
            return bcrypt.checkpw(password.encode("utf-8"), hashed_password)
        except Exception:
            return False
    def _revoke_jti(self, jti: str):
        query = self.revoked_table.insert().values(jti=jti)
        self.sql_client.query(query)
    def _is_jti_revoked(self, jti: str):
        query = (
            self.revoked_table.select()
            .columns("jti")
            .where(Condition(self.revoked_table.jti, "=", jti))
            .limit(1)
        )
        return bool(self.economizador.scalar(query))
    def login(self, user_form: UserForm, escena: str = None):
        query = (
            self.users_table.select()
            .columns("id", "password")
            .where(Condition(self.users_table.username, "=", user_form.username))
            .limit(1)
        )
        row = self.economizador.fetchone(query)
        if not row:
            self._is_password_correct(None, user_form.password)
            return None
        user_id, hashed_password = row
        if not self._is_password_correct(hashed_password, user_form.password):
            return None
        session = Session(_user=str(user_id), _escena=escena)
        tokens = self.jwt_manager.create_token_pair(
            username=str(user_id),
            jti=str(uuid4())
        )
        return session, tokens
    def register(self, user_form: UserForm, escena: str = None):
        data = asdict(user_form)
        password = data.pop("password")
        data["password"] = bcrypt.hashpw(
            password.encode("utf-8"),
            bcrypt.gensalt(rounds=12)
        )
        query = self.users_table.insert().values(**data).returning("id")
        user_id = self.sql_client.scalar(query)
        session = Session(_user=str(user_id), _escena=escena)
        tokens = self.jwt_manager.create_token_pair(
            username=str(user_id),
            jti=str(uuid4())
        )
        return session, tokens
    def delete(self, user_id: int):
        query = (
            self.users_table.delete()
            .where(Condition(self.users_table.id, "=", user_id))
        )
        self.economizador.delete(query)
        return True
    def update_user(self, user_form: UserUpdateForm):
        data = asdict(user_form)
        user_id = data.pop("id")
        data = {k: v for k, v in data.items() if v is not None}
        if not data:
            return False
        if "password" in data:
            data["password"] = bcrypt.hashpw(
                data["password"].encode("utf-8"),
                bcrypt.gensalt(rounds=12)
            )
        query = (
            self.users_table.update()
            .set(**data)
            .where(Condition(self.users_table.id, "=", user_id))
        )
        self.economizador.update(query)
        return True
    def is_session_logged(self, request: StandarRequest, escena: str = None):
        token = request.cookies.get("sessionID")
        if not token:
            return None
        user_id = self.jwt_manager.whois(token)
        query = (
            self.users_table.select()
            .columns("id")
            .where(Condition(self.users_table.id, "=", user_id))
            .limit(1)
        )
        exists = self.economizador.scalar(query)
        if not exists:
            return None
        return Session(_user=str(user_id), _escena=escena)
    def refresh(self, refresh_token: str):
        payload = self.jwt_manager.extract_refresh_payload(refresh_token)
        if self._is_jti_revoked(payload["jti"]):
            return None
        user_id = payload["sub"]
        query = (
            self.users_table.select()
            .columns("id")
            .where(Condition(self.users_table.id, "=", user_id))
            .limit(1)
        )
        exists = self.economizador.scalar(query)
        if not exists:
            return None
        self._revoke_jti(payload["jti"])
        tokens = self.jwt_manager.create_token_pair(
            username=str(user_id),
            jti=str(uuid4())
        )
        return tokens
    def logout(self, refresh_token: str):
        payload = self.jwt_manager.extract_refresh_payload(refresh_token)
        self._revoke_jti(payload["jti"])
        return True