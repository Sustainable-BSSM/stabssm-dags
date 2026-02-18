from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class Requester(ABC):

    @abstractmethod
    def get(
            self,
            url : str,
            headers : Optional[Dict[str, Any]] = None,
            params : Optional[Dict[str, Any]] = None,
            cookies : Optional[Dict[str, Any]] = None,
    ):
        raise NotImplementedError