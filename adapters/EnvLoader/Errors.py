class BaseError(Exception):pass

class EnvManagerError(BaseError): 
    description = "Error relacionado al entorno"
    pass

class AdapterError(BaseError):
    pass

class ClassInitializationError(BaseError):
    pass

class ClassConstructionError(BaseError):
    pass