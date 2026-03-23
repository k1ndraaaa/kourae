from adapters.EnvLoader.Errors import *

class _PostgresClientBaseError(BaseError): pass
class PostgresClientError(_PostgresClientBaseError): pass