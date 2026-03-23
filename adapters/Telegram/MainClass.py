from adapters.Telegram.Errors import *
import requests

class TelegramNotifier:
    def __init__(
        self,
        http_host: str
    ):
        self.http_host = http_host
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