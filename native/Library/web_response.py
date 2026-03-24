class WebError:
    def __init__(
        self,
        message: str,
        code: str | None = None,
        details=None
    ):
        self.message = message
        self.code = code
        self.details = details
    def to_dict(self) -> dict:
        payload = {
            "message": self.message
        }
        if self.code is not None:
            payload["code"] = self.code
        if self.details is not None:
            payload["details"] = self.details
        return payload
class WebResponse:
    def __init__(
        self,
        status: int = 200,
        data=None
    ):
        self.status = status
        self.data = data
        self.error: WebError | None = None
        self.meta: dict = {}
        self.headers: dict = {}
    @property
    def ok(self) -> bool:
        return self.status < 400
    def success(
        self,
        data=None,
        status: int = 200
    ):
        self.status = status
        self.data = data
        self.error = None
        return self
    def fail(
        self,
        message: str,
        status: int = 400,
        *,
        code: str | None = None,
        details=None
    ):
        self.status = status
        self.data = None
        self.error = WebError(
            message=message,
            code=code,
            details=details
        )
        return self
    def bad_request(self, message="Bad request"):
        return self.fail(message, 400)
    def unauthorized(self, message="Unauthorized"):
        return self.fail(message, 401)
    def forbidden(self, message="Forbidden"):
        return self.fail(message, 403)
    def not_found(self, message="Not found"):
        return self.fail(message, 404)
    def conflict(self, message="Conflict"):
        return self.fail(message, 409)
    def server_error(self, message="Internal server error"):
        return self.fail(message, 500)
    def add_meta(self, key: str, value):
        self.meta[key] = value
        return self
    def add_header(self, key: str, value):
        self.headers[key] = value
        return self
    def from_exception(
        self,
        exc: Exception,
        status: int = 500
    ):
        return self.fail(
            message=str(exc),
            status=status,
            code=exc.__class__.__name__
        )
    def to_dict(self) -> dict:
        payload = {
            "ok": self.ok,
            "status": self.status
        }
        if self.data is not None:
            payload["data"] = self.data
        if self.error is not None:
            payload["error"] = self.error.to_dict()
        if self.meta:
            payload["meta"] = self.meta
        return payload
    def export(self) -> dict:
        return {
            "status": self.status,
            "headers": self.headers,
            "body": self.to_dict()
        }