class BaseError(Exception):pass

class EnvLoaderError(BaseError): 
    description = "Error relacionado al entorno"
    pass

class AdapterError(BaseError):
    pass

class ClassInitializationError(BaseError):
    pass

class ClassConstructionError(BaseError):
    pass