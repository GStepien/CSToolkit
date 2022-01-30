# cs = concurrency safety
from __future__ import annotations

import abc
import warnings
from enum import IntEnum, auto
from typing import Any, final, runtime_checkable, Protocol, Final, Union, Optional

"""
CS = Concurrency Safe(ty)
CSL = CS level
"""


class En:
    class CSL(IntEnum):
        SINGLE_THREAD = auto()
        MULTI_THREAD = auto()
        MULTI_PROCESS = auto()


class Pr:
    @runtime_checkable
    class HasCSLMixin(Protocol):

        @abc.abstractmethod
        def c_get_csl(self) -> En.CSL:
            ...


class Im:
    class HasCSLMixin(Pr.HasCSLMixin):
        __csl: En.CSL

        def __init__(self,
                     *args: Any,
                     csl: En.CSL,
                     **kwargs: Any):
            self.__csl = csl
            super().__init__(*args, **kwargs)  # type: ignore

        @final
        def c_get_csl(self) -> En.CSL:
            return self.__csl


class Er:
    class CSLError(Exception):
        pass

    class CSLIncompatibleError(CSLError):
        pass


class Ca:
    @staticmethod
    def c_get_csl(obj: ANY_CSL) -> En.CSL:
        return (obj
                if isinstance(obj, En.CSL)
                else obj.c_get_csl())

    @staticmethod
    def c_are_cs_compatible(accessor: ANY_CSL,
                            accessee: ANY_CSL,
                            raise_if_incompatible: bool = False,
                            warn_if_redundant: bool = False,
                            msg: Optional[str] = None) -> bool:
        accessor_csl: Final[En.CSL] = Ca.c_get_csl(accessor)
        accessee_csl: Final[En.CSL] = Ca.c_get_csl(accessee)

        if accessee_csl >= accessor_csl:
            if warn_if_redundant and accessee_csl > accessor_csl:
                warnings.warn(f"Accessee has a larger CSL ({accessee_csl.name}) than "
                              f"the accessor ({accessor_csl.name}).")
            return True
        elif raise_if_incompatible:
            raise (Er.CSLIncompatibleError()
                   if msg is None
                   else Er.CSLIncompatibleError(msg))
        else:
            return False


ANY_CSL = Union[En.CSL, Pr.HasCSLMixin]
