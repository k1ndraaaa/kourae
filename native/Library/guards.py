from contextvars import ContextVar
from native.Library.commons import Request as StandarRequest
from native.Library.web_response import WebResponse
_current_ctx: ContextVar["RequestContext"] = ContextVar("request_context")
def set_request_context(ctx):
    _current_ctx.set(ctx)
def ctx():
    return _current_ctx.get()
def request():
    return ctx().request
def response():
    return ctx().response
def vm():
    return ctx().vm
class RequestContext:
    def __init__(
        self,
        request: StandarRequest,
        vm
    ):
        self.request = request
        self.vm = vm
        self.response = WebResponse
        self.user = None
        self.ip = None
        self.meta = {}
class Guard:
    def run(self, ctx):
        return True, None
class AuthGuard(Guard):
    def run(self, ctx):
        session = ctx.vm.auth_manager.is_session_logged(ctx.request)
        if not session:
            return False, ctx.response.fail(
                "No hay una sesión activa",
                401,
                code="UNAUTHORIZED"
            )
        ctx.user = session
        return True, None
class ContentTypeGuard(Guard):
    def __init__(self, expected):
        self.expected = expected
    def run(self, ctx):
        if not ctx.request.is_json():
            return False, ctx.response.fail(
                "Content-Type inválido",
                415,
                code="UNSUPPORTED_MEDIA_TYPE"
            )
        return True, None
class HeaderGuard(Guard):
    def __init__(self, headers):
        self.headers = headers
    def run(self, ctx):
        missing = []
        for h in self.headers:
            if h.lower() not in ctx.request.headers:
                missing.append(h)
        if missing:
            return False, ctx.response.fail(
                "Faltan headers",
                400,
                code="MISSING_HEADERS"
            )
        return True, None
class GuardPipeline:
    def __init__(self, guards):
        self.guards = guards
    def run(self, ctx):
        for guard in self.guards:
            ok, resp = guard.run(ctx)
            if not ok:
                return False, resp
        return True, None