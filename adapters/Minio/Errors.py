from adapters.EnvLoader.Errors import *

class _MinioClientBaseError(BaseError): pass
class MinioClientError(_MinioClientBaseError): pass