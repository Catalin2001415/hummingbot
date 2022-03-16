import hmac
import json
import urllib
import hashlib
from typing import Dict, Any


class ExmoAuth():
    """
    Auth class required by Exmo API
    """
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def get_headers(
        self,
        timestamp: int = None,
        params: Dict[str, Any] = {}
    ):
        """
        Generates authenticated headers for the request.
        :return: a dictionary of auth headers
        """

        params['nonce'] = timestamp
        payload =  urllib.parse.urlencode(params)

        sign = hmac.new(
            self.secret_key.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha512
        ).hexdigest()

        return {
            "Content-Type": "application/x-www-form-urlencoded",
            "key": self.api_key,
            "sign": sign,
        }

    def get_ws_auth_payload(self, timestamp: int = None):
        """
        Generates websocket payload.
        :return: a dictionary of auth headers with api_key, timestamp, signature
        """

        payload = f'{str(timestamp)}#{self.memo}#exmo.WebSocket'

        sign = hmac.new(
            self.secret_key.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        return {
            "op": "login",
            "args": [
                self.api_key,
                str(timestamp),
                sign
            ]
        }
