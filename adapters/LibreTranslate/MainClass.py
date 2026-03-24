from adapters.EnvLoader.MainClass import EnvLoader, root_path
from adapters.LibreTranslate.Errors import *
from pathlib import Path
import requests

class LibreTranslateClient:
    def __init__(self):
        mypath = Path(root_path / "adapters" / "Telegram")
        self.env = EnvLoader().load_vars_from_env(
            path= Path(mypath / ".env")
        )
        self.http_host = str(self.env.get("http_host"))
        self.server_language = str(self.env.get("server_language"))
        
    def translate(
        self, 
        text: str = None, 
        target: str ="en"
    ):
        try:
            r = requests.post(
                f"{self.url}/translate",
                json={
                    "q": text,
                    "source": self.server_language,
                    "target": target,
                    "format": "text"
                },
                timeout=10
            )
            r.raise_for_status()
            return r.json()["translatedText"]
        except Exception:
            return text