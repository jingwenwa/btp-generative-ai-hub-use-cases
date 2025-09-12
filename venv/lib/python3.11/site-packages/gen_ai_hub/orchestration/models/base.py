from abc import ABC, abstractmethod
from typing import Dict, Any


class JSONSerializable(ABC):
    """
    An interface for objects that can be serialized to JSON.
    """

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert the object to a JSON-serializable dictionary."""
        pass
