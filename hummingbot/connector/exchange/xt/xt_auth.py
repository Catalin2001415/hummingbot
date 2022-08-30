import hmac
import json
import urllib
import hashlib
from typing import Dict, Any


class XtAuth():
    """
    Auth class required by XT API
    Learn more at https://doc.xt.com/#documentationsignStatement
    """
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def get_auth_dict(
        self,
        timestamp: int = None,
        params: Dict[str, Any] = None,
    ):
        """
        Generates auth dictionary.
        :return: an authorization dictionary
        """

        params["accesskey"] = self.api_key
        params["nonce"] = str(timestamp)
        payload = urllib.parse.urlencode(dict(sorted(params.items(), key = lambda kv:(kv[0], kv[1]))))

        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest().upper()

        params["signature"] = signature

        return params
