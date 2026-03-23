from native.Library.commons import Request as StandarRequest, Field
from collections.abc import Sized

class ExpectedData:
    def __init__(self, request: StandarRequest):
        self.fields: dict[str, Field] = {}
        self.standar_request = request
        self._captured: dict = {}
        self._errors: dict = {}
    def __iter__(self):
        return iter(self.fields.values())
    def add(self, field: Field):
        if field.key in self.fields:
            raise ValueError(f"Field '{field.key}' duplicado")
        self.fields[field.key] = field
        return self
    def scan(self):
        data = self.standar_request.get_data()
        if not isinstance(data, dict):
            self._errors = {"body": "INVALID_BODY"}
            self._captured = {}
            return {}
        captured = {}
        errors = {}
        for field in self.fields.values():
            value = data.get(field.key)
            if value is None:
                if field.default is not None:
                    value = field.default() if callable(field.default) else field.default
                    captured[field.key] = value
                    continue
                errors[field.key] = "MISSING"
                continue
            if field.datatype is not None and not isinstance(value, field.datatype):
                errors[field.key] = "INVALID_DATATYPE"
                continue
            if (
                field.min_length is not None
                and isinstance(value, Sized)
                and len(value) < field.min_length
            ):
                errors[field.key] = "MIN_LENGTH"
                continue
            if (
                field.max_length is not None
                and isinstance(value, Sized)
                and len(value) > field.max_length
            ):
                errors[field.key] = "MAX_LENGTH"
                continue
            if isinstance(field.scanner, (list, tuple, set)):
                if value not in field.scanner:
                    errors[field.key] = "INVALID_OPTION"
                    continue
            elif callable(field.scanner):
                if not field.scanner.validate_string(value).valido:
                    errors[field.key] = "INVALID_VALUE"
                    continue
            captured[field.key] = value
        self._captured = captured
        self._errors = errors
        return captured
    def errors(self):
        return self._errors
    def is_valid(self):
        return not bool(self._errors)
    def data(self):
        return self._captured
