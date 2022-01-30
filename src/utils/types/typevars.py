import abc
import enum
from numbers import Number
from typing import TypeVar, Any, Callable, runtime_checkable, Protocol


@runtime_checkable
class _Hashable(Protocol):
    """
    We cannot use Hashable from utils.types.magicmethods due to circular import
    """
    @abc.abstractmethod
    def __hash__(self, /) -> int:
        ...


SELF = TypeVar('SELF')

ENUM = TypeVar('ENUM', bound=enum.Enum)

TYPE = TypeVar('TYPE')
TYPE_co = TypeVar('TYPE_co', covariant=True)
TYPE_contra = TypeVar('TYPE_contra', contravariant=True)

TYPE2 = TypeVar('TYPE2')
TYPE2_co = TypeVar('TYPE2_co', covariant=True)
TYPE2_contra = TypeVar('TYPE2_contra', contravariant=True)

HASHABLE = TypeVar('HASHABLE', bound=_Hashable)

K_TYPE = TypeVar('K_TYPE', bound=_Hashable)
K_TYPE_co = TypeVar('K_TYPE_co', bound=_Hashable, covariant=True)
K_TYPE_contra = TypeVar('K_TYPE_contra', bound=_Hashable, contravariant=True)

V_TYPE = TypeVar('V_TYPE')
V_TYPE_co = TypeVar('V_TYPE_co', covariant=True)
V_TYPE_contra = TypeVar('V_TYPE_contra', contravariant=True)

CALLABLE_TYPE = TypeVar('CALLABLE_TYPE', bound=Callable[..., Any])

NUM_TYPE = TypeVar('NUM_TYPE', bound=Number)
