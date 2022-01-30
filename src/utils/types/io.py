import abc
from typing import Protocol, Any, runtime_checkable

from utils.types.typevars import TYPE_contra


class Pr:
    @runtime_checkable
    class SupportsWrite(Protocol[TYPE_contra]):
        @abc.abstractmethod
        def write(self,
                  item: TYPE_contra) -> Any:
            ...
