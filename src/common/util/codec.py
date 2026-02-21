import json
from abc import abstractmethod
from typing import Protocol, Any


class Serializer(Protocol):

    @staticmethod
    @abstractmethod
    def serialize(obj: Any) -> Any:
        raise NotImplementedError

class Deserializer(Protocol):

    @staticmethod
    @abstractmethod
    def deserialize(serialized_obj: Any) -> Any:
        raise NotImplementedError

class JsonSerializer:

    @staticmethod
    def serialize(obj: Any) -> Any:
        serialized_obj = json.dumps(obj, ensure_ascii=False)
        return serialized_obj

class JsonDeserializer:

    @staticmethod
    def deserialize(serialized_obj: Any) -> Any:
        return json.loads(serialized_obj)