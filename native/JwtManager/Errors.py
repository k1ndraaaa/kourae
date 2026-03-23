from adapters.EnvLoader.Errors import BaseError

class TokenError(BaseError): pass
class TokenExpired(TokenError): pass
class TokenInvalid(TokenError): pass
class TokenTypeMismatch(TokenError): pass