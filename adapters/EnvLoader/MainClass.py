#Componentes a usar
from dotenv import load_dotenv, dotenv_values #type:ignore
from adapters.EnvLoader.Errors import *
from pathlib import Path
import os

root_str_path = "/home/kourae/Documents/kourae-net"
root_path = Path(root_str_path)

class EnvLoader:
    def __init__(self):
        pass
    def load_vars_from_env(
        self, 
        path:Path=None,
        inject:bool=False
    ):
        env = {}
        if not os.path.exists(path):
            raise EnvManagerError(f"Env path no existe")
        env_vars = dotenv_values(path)
        if inject:
            load_dotenv(dotenv_path=path, override=True)
        for k, v in env_vars.items():
            env[k] = v
        return env
    #El contexto de esta función se está limitando a python, posiblemente la modifique después.
    def path_to_pypath(
        self,
        path:str, 
        package_root:str
    ):
        path = Path(path).resolve()
        package_root = Path(package_root).resolve()
        if not path.is_file():
            raise EnvManagerError(f"No es un archivo válido: {path}")
        if path.suffix != ".py":
            raise EnvManagerError(f"No es un archivo Python: {path}")
        try:
            relative = path.relative_to(package_root)
        except ValueError:
            raise EnvManagerError("El archivo no está dentro del package_root")
        return ".".join(relative.with_suffix("").parts)
    def scan_directory(self, directory: Path, root_path: Path, ignored_dirs=None, ignored_files=None):
        if ignored_dirs is None: ignored_dirs = []
        if ignored_files is None: ignored_files = ["vm.py", "main.py", "__init__.py"]
        content = {}
        for item in directory.iterdir():
            if item.is_dir() and item.name not in ignored_dirs:
                content[item.name] = {"type": "dir", "source": item.as_posix()}
            elif item.is_file() and item.suffix == ".py" and item.name not in ignored_files:
                content[item.name] = {
                    "type": "module",
                    "source": item.as_posix(),
                    "pypath": self.path_to_pypath(item.as_posix(), root_path)
                }
        return content