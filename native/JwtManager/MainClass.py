from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict
import jwt
from pathlib import Path
from native.JwtManager.Errors import TokenExpired, TokenInvalid, TokenTypeMismatch
from adapters.EnvLoader.MainClass import EnvLoader, root_path

class JwtManager:
    ISSUER = "kourae-api"
    def __init__(self):
        mypath = Path(root_path / "native" / "JwtManager")
        env = EnvLoader().load_vars_from_env(
            path= Path(mypath / ".env")
        )
        config = {
            "jwt_secret_key": env["jwt_secret_key"],
            "jwt_algorithm": env.get("jwt_algorithm", "HS256"),
            "access_token_minutes": int(env.get("access_token_minutes", 15)),
            "refresh_token_days": int(env.get("refresh_token_days", 7)),
            "utc_zone": ZoneInfo("UTC"),
        }
        try:
            self.jwtsecret = config["jwt_secret_key"]
        except KeyError:
            raise ValueError("Falta 'jwt_secret_key' en configuración JWT")
        self.jwtalgorithm = config.get("jwt_algorithm", "HS256")
        self.access_token_minutes = int(config.get("access_token_minutes", 15))
        self.refresh_token_days = int(config.get("refresh_token_days", 7))
        self.utczone = config.get("utc_zone", ZoneInfo("UTC"))
    def encode(self, payload: Dict, expires_delta: timedelta) -> str:
        now = datetime.now(tz=self.utczone)
        data = payload.copy()
        data.update({
            "iat": int(now.timestamp()),
            "exp": int((now + expires_delta).timestamp()),
            "iss": self.ISSUER,
        })
        token = jwt.encode(data, self.jwtsecret, algorithm=self.jwtalgorithm)
        return token
    def decode(self, token: str, verify_exp: bool = True) -> Dict:
        try:
            return jwt.decode(
                token,
                self.jwtsecret,
                algorithms=[self.jwtalgorithm],
                options={"verify_exp": verify_exp},
                leeway=5,
                issuer=self.ISSUER,
            )
        except jwt.ExpiredSignatureError:
            raise TokenExpired("Token expirado")
        except jwt.InvalidTokenError as e:
            raise TokenInvalid(str(e))
    def create_access_token(self, username: str) -> str:
        payload = {
            "sub": str(username),
            "type": "access",
        }
        return self.encode(payload, timedelta(minutes=self.access_token_minutes))
    def create_refresh_token(self, username: str, jti: str) -> str:
        payload = {
            "sub": str(username),
            "type": "refresh",
            "jti": jti,
        }
        return self.encode(payload, timedelta(days=self.refresh_token_days))
    def create_token_pair(self, username: str, jti: str) -> Dict[str, str]:
        return {
            "sessionID": self.create_access_token(username),
            "refresh_sessionID": self.create_refresh_token(username, jti),
        }
    def validate_token(self, token: str, expected_type: str) -> Dict:
        payload = self.decode(token)
        token_type = payload.get("type")
        if token_type != expected_type:
            raise TokenTypeMismatch(
                f"Se esperaba token '{expected_type}', llegó '{token_type}'"
            )
        sub = payload.get("sub")
        if not sub:
            raise TokenInvalid("Token sin subject")
        return payload
    def is_token_expired(self, token: str) -> bool:
        try:
            self.decode(token, verify_exp=True)
            return False
        except TokenExpired:
            return True
    def whois(self, token: str) -> str:
        payload = self.validate_token(token, expected_type="access")
        return payload["sub"]
    def extract_refresh_payload(self, refresh_token: str) -> Dict:
        return self.validate_token(refresh_token, expected_type="refresh")
    def refresh_access_token(self, refresh_token: str):
        payload = self.extract_refresh_payload(refresh_token)
        username = payload["sub"]
        jti = payload["jti"]
        new_access = self.create_access_token(username)
        return new_access