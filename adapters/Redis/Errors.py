from adapters.EnvLoader.Errors import *

class _RedisBaseError(BaseError): pass
class RedisClientError(_RedisBaseError): pass