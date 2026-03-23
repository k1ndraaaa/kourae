from typing import BinaryIO, Protocol, Dict, Optional, Mapping, Union, Any, Callable, Tuple
from io import BytesIO, IOBase, BufferedIOBase, RawIOBase
from dataclasses import dataclass, field
from types import MappingProxyType
from cgi import parse_header
import hashlib, json, base64, importlib, re
from native.PayloadValidator.MainClass import PayloadValidator 

def build_set(data):
    parts = []
    params = []
    for key, value in data.items():
        parts.append(f"{key} = %s")
        params.append(value)
    return ", ".join(parts), params
#helpers de cross framework
def _freeze_mapping(data: Optional[Mapping]) -> Mapping:
    if not data:
        return MappingProxyType({})
    return MappingProxyType(dict(data))
def _normalize_headers(headers: Optional[Mapping[str, str]]) -> Mapping[str, str]:
    if not headers:
        return MappingProxyType({})
    return MappingProxyType({k.lower(): v for k, v in headers.items()})
def _is_instance_of(obj, module_name, class_name):
    try:
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return isinstance(obj, cls)
    except ImportError:
        return False
def _check(obj, module_name, class_name):
    return _is_instance_of(obj, module_name, class_name)
#función usable o rescatable del módulo
def to_binary_io(file_obj: Union[bytes, bytearray, BinaryIO, object]) -> BinaryIO:
    """
        Normaliza diferentes tipos de entrada a un BinaryIO válido.
        Soporta:
            - bytes o bytearray
            - IOBase (archivos abiertos en modo binario)
            - Wrappers comunes que exponen `.file` o `.stream`
            - Objetos con método `.read()` que retornen bytes
        No depende de ningún framework específico.
    """
    if isinstance(file_obj, (bytes, bytearray)):
        return BytesIO(file_obj)
    if isinstance(file_obj, (BufferedIOBase, RawIOBase)):
        return file_obj
    if isinstance(file_obj, IOBase):
        if getattr(file_obj, "encoding", None) is not None:
            raise TypeError("Se recibió un stream de texto, pedí binario!!")
        return file_obj
    for attr in ("file", "stream"):
        candidate = getattr(file_obj, attr, None)
        if isinstance(candidate, (BufferedIOBase, RawIOBase)):
            return candidate
    read_method = getattr(file_obj, "read", None)
    if callable(read_method):
        try:
            probe = read_method(0)
            if isinstance(probe, bytes):
                return file_obj
        except Exception:
            pass
    raise TypeError(
        f"Tipo de archivo no soportado: {type(file_obj).__name__}"
    )
def _normalize_multidict(data):
    normalized = {}
    for key, value in data.items():
        if isinstance(value, list):
            normalized[key] = value[0] if len(value) == 1 else value
        else:
            normalized[key] = value
    return normalized
def human_readable_size(size_bytes: int):
    if size_bytes == 0:
        return "0B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(units)-1:
        size_bytes /= 1024
        i += 1
    return f"{size_bytes:.2f}{units[i]}"
def b64_encrypt(text: str, key: str) -> str:
    text_bytes = text.encode()
    key_bytes = key.encode()
    encrypted_bytes = bytes([b ^ key_bytes[i % len(key_bytes)] for i, b in enumerate(text_bytes)])
    return base64.urlsafe_b64encode(encrypted_bytes).decode()
def b64_decrypt(token: str, key: str) -> str:
    encrypted_bytes = base64.urlsafe_b64decode(token.encode())
    key_bytes = key.encode()
    decrypted_bytes = bytes([b ^ key_bytes[i % len(key_bytes)] for i, b in enumerate(encrypted_bytes)])
    return decrypted_bytes.decode()

class Row:
    ...

class UserForm(Protocol):
    #id lo produce db, así que ni lo pongo
    username: str
    password: str | bytes

class UserUpdateForm(Protocol):
    #id lo consultamos manualmente
    username: str | None
    password: str | bytes | None

class FileForm(Protocol):
    user_id: int #inmutable

    filename: str 
    ext: str #inmutable
    
    mime_type: str #inmutable
    size: int #inmutable
    
    bucket: str #inmutable
    object_key: str #inmutable

class FileUpdateForm(Protocol):
    user_id: int #Sólo se pasa como referencia al archivo guardado
    filename: str

@dataclass(frozen=True)
class StorageObject:
    bucket: str
    object_key: str
    data: BinaryIO
    length: int
    mime_type: str

@dataclass(frozen=True)
class StoragePointer:
    bucket: str
    object_key: str

@dataclass(frozen=True)
class FileMeta:
    user_id: int
    filename: str
    privacy: str
    ext: str
    mime_type: str
    size: int
    bucket: str
    object_key: str

class SqlClient:
    pass

@dataclass
class ColumnSchema:
    name: str
    type: str
    nullable: bool

@dataclass
class TableSchema:
    name: str
    columns: Dict[str, str]


@dataclass
class DatabaseSchema:
    id: str
    tables: Dict[str, TableSchema] = field(default_factory=dict)
    version: str = ""
    def compute_version(self):
        structure = {
            table_name: table.columns
            for table_name, table in sorted(self.tables.items())
        }
        serialized = json.dumps(structure, sort_keys=True)
        self.version = hashlib.sha256(serialized.encode()).hexdigest()


@dataclass(frozen=True)
class Session:
    _user: Optional[str] = field(default="python")
    _escena: Optional[str] = field(default="main")
    password: Optional[str] = field(default="123abc")
    address: Optional[str] = field(default=None)
    def __post_init__(self):
        if self.address is None and self._user and self._escena and self.password:
            object.__setattr__(
                self,
                "address",
                b64_encrypt(
                    text=f"{self._user}@{self._escena}",
                    key=self.password
                )
            )
    @property
    def user(self):
        if self._user is not None:
            return self._user
        if self.address and self.password:
            try:
                decrypted = b64_decrypt(self.address, key=self.password)
                return decrypted.split("@", 1)[0]
            except Exception:
                return None
        return None
    @property
    def escena(self):
        if self._escena is not None:
            return self._escena
        if self.address and self.password:
            try:
                decrypted = b64_decrypt(self.address, key=self.password)
                return decrypted.split("@", 1)[1]
            except Exception:
                return None
        return None
    def __iter__(self):
        for key in ["user", "escena", "password", "address"]:
            yield (key, getattr(self, key))

#objetos de la solicitud
@dataclass(frozen=True, slots=True)
class Client:
    ip: Optional[str] = None
    port: Optional[int] = None
    user_agent: Optional[str] = None

@dataclass(frozen=True, slots=True)
class Auth:
    type: Optional[str] = None
    credentials: Any = None

TranslatorEntry = Tuple[
    Callable[[Any], bool],# check
    Callable[[Any], Any],# traductor a usar
    bool # async sí o no
]

@dataclass(frozen=True, slots=True)
class Request:
    method: str
    url: str
    path: str
    headers: Mapping[str, str] = field(default_factory=dict)
    query_params: Mapping[str, Any] = field(default_factory=dict)
    path_params: Mapping[str, Any] = field(default_factory=dict)
    body: Any = None
    form: Mapping[str, Any] = field(default_factory=dict)
    files: Mapping[str, Any] = field(default_factory=dict)
    cookies: Mapping[str, str] = field(default_factory=dict)
    client: Client = field(default_factory=Client)
    auth: Auth = field(default_factory=Auth)
    meta: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, "method", self.method.upper())
        object.__setattr__(self, "headers", _normalize_headers(self.headers))
        object.__setattr__(self, "query_params", _freeze_mapping(self.query_params))
        object.__setattr__(self, "path_params", _freeze_mapping(self.path_params))
        object.__setattr__(self, "form", _freeze_mapping(self.form))
        object.__setattr__(self, "files", _freeze_mapping(self.files))
        object.__setattr__(self, "cookies", _freeze_mapping(self.cookies))
        object.__setattr__(self, "meta", _freeze_mapping(self.meta))

    def header(self, name: str, default: Any = None) -> Any:
        return self.headers.get(name.lower(), default)
    def query(self, name: str, default: Any = None) -> Any:
        return self.query_params.get(name, default)    
    def get_files(self) -> dict[str, list[dict]]:
        result = {}
        for field_name, raw_files in self.files.items():
            if not isinstance(raw_files, list):
                raw_files = [raw_files]
            file_list = []
            for f in raw_files:
                stream = f.get("stream")
                if stream:
                    try:
                        stream = to_binary_io(stream)
                    except TypeError:
                        pass
                file_list.append({
                    "filename": f.get("filename"),
                    "content_type": f.get("content_type"),
                    "stream": stream,
                })
            result[field_name] = file_list
        return result
    def _json(self, default: Any = None, *, silent: bool = False) -> Any:
        if self.body is None:
            return default
        if isinstance(self.body, (dict, list)):
            return self.body
        if isinstance(self.body, (str, bytes)):
            try:
                return json.loads(self.body)
            except json.JSONDecodeError:
                if silent:
                    return default
                raise
        return default
    def _formdata(self):
        if not self.form:
            return {}
        return _normalize_multidict(self.form)
    def get_data(self, default: Any = None):
        if self.is_json():
            return self._json(default=default)
        if self.is_multipart_formdata() or self.is_form_urlencoded():
            return self._formdata()
        return default
    def is_json(self) -> bool:
        content_type = self.header("content-type", "")
        mime_type, _ = parse_header(content_type)
        mime_type = mime_type.lower()
        return mime_type == "application/json" or mime_type.endswith("+json")
    def is_multipart_formdata(self) -> bool:
        content_type = self.header("content-type", "")
        mime_type, params = parse_header(content_type)
        return (
            mime_type.lower() == "multipart/form-data"
            and "boundary" in params
        )
    def is_form_urlencoded(self) -> bool:
        content_type = self.header("content-type", "")
        mime_type, _ = parse_header(content_type)
        return mime_type.lower() == "application/x-www-form-urlencoded"
    
@dataclass
class Field:
    key: str
    min_length: int | None = None
    max_length: int | None = None
    datatype: type | tuple[type, ...] | None = None
    scanner: tuple | PayloadValidator | None = None
    default: object | None = None

class Condition:
    def __init__(self, column, operator=None, value=None, children=None, combiner=None):
        self.column = column
        self.operator = operator
        self.value = value
        self.children = children
        self.combiner = combiner
    # AND
    def __and__(self, other):
        return Condition(
            column=None,
            children=[self, other],
            combiner="AND"
        )
    # OR
    def __or__(self, other):
        return Condition(
            column=None,
            children=[self, other],
            combiner="OR"
        )

class Column:
    def __init__(self, table, name, dtype):
        self.table = table
        self.name = name
        self.dtype = dtype

    def __str__(self):
        return f"{self.table.name}.{self.name}"

    def __eq__(self, other):
        return Condition(self, "=", other)

    def __gt__(self, other):
        return Condition(self, ">", other)

    def __lt__(self, other):
        return Condition(self, "<", other)

    def __ge__(self, other):
        return Condition(self, ">=", other)

    def __le__(self, other):
        return Condition(self, "<=", other)
    
def _build_single_condition(cond: Condition):
    if cond.children:
        child_sqls = []
        child_params = []
        for child in cond.children:
            s, p = _build_single_condition(child)
            child_sqls.append(s)
            child_params.extend(p)
        return f"({' {} '.format(cond.combiner).join(child_sqls)})", child_params
    else:
        col = cond.column
        op = cond.operator
        val = cond.value
        if isinstance(val, Column):
            return f"{col} {op} {val}", []
        else:
            return f"{col} {op} %s", [val]

def build_conditions(conditions):
    sql_parts = []
    params = []
    for cond in conditions:
        part_sql, part_params = _build_single_condition(cond)
        sql_parts.append(part_sql)
        params.extend(part_params)
    return " AND ".join(sql_parts), params