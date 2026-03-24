from adapters.Telegram.Errors import *
from adapters.EnvLoader.MainClass import EnvLoader, root_path
import requests
from pathlib import Path

class TelegramNotifier:
    def __init__(
        self
    ):
        mypath = Path(root_path / "adapters" / "Telegram")
        self.env = EnvLoader().load_vars_from_env(
            path= Path(mypath / ".env")
        )
        self.http_host = str(self.env.get("http_host"))
    def send(
        self, 
        reciever: str,
        message: str
    ):
        try:
            requests.post(
                url=self.http_host,
                json={
                    "reciever": reciever, 
                    "message": message
                }
            )
        except Exception as e:
            raise TelegramError(e)