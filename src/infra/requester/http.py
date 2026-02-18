from typing import Optional, Dict, Any

import requests

from src.core.requester import Requester


class HttpRequester(Requester):
    def get(
            self,
            url: str,
            headers: Optional[Dict[str, Any]] = None,
            params: Optional[Dict[str, Any]] = None,
            cookies: Optional[Dict[str, Any]] = None
    ):
        data = requests.get(
            url=url,
            headers=headers,
            params=params,
            cookies=cookies
        )
        return data
