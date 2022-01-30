# gmm

import abc
from functools import total_ordering
from types import TracebackType
from typing import runtime_checkable, Protocol, Optional, Type, Any, final, Generic, cast, Final, Literal

from utils.types.typevars import TYPE_contra, TYPE_co, SELF, TYPE2_contra


class Pr:

    @runtime_checkable
    class Hashable(Protocol):

        @abc.abstractmethod
        def __hash__(self, /) -> int:
            ...

    @runtime_checkable
    class SupportsLessThan(Protocol[TYPE_contra]):

        @abc.abstractmethod
        def __lt__(self,
                   value: TYPE_contra, /) -> bool:
            ...

    @runtime_checkable
    class TotallyOrdered(SupportsLessThan[TYPE_contra],
                         Protocol[TYPE_contra]):

        @abc.abstractmethod
        def __le__(self,
                   value: TYPE_contra, /) -> bool:
            ...

        @abc.abstractmethod
        def __gt__(self,
                   value: TYPE_contra, /) -> bool:
            ...

        @abc.abstractmethod
        def __ge__(self,
                   value: TYPE_contra, /) -> bool:
            ...

    @runtime_checkable
    class ContextManager(Protocol[TYPE_co]):

        @abc.abstractmethod
        def __enter__(self, /) -> TYPE_co:
            ...

        @abc.abstractmethod
        def __exit__(self,
                     exc_type: Optional[Type[BaseException]],
                     exc_value: Optional[BaseException],
                     exc_traceback: Optional[TracebackType], /) -> Optional[bool]:
            """
            In case of exception (i.e., exc_* are not None), return 'truthy' value iff exception
            should be suppressed.
            Should not re-raise passed exception.
            """
            ...

    @runtime_checkable
    class SelfContextManager(ContextManager[Any], Protocol):

        @abc.abstractmethod
        def __enter__(self: SELF, /) -> SELF:
            ...


class Ab:

    class SupportsEquals:
        """
        Note: Overriding __eq__ implicitly sets __hash__ to None unless __hash__ is
        explicitly overwritten.
        """

        @final
        def __eq__(self,
                   value: Any, /) -> bool:
            if self is value:
                return True
            else:
                result: bool = self._c_eq(value)

                assert (not result or
                        not isinstance(self, Pr.Hashable) or
                        not isinstance(value, Pr.Hashable) or
                        hash(self) == hash(value))

                return result

        @final
        def __ne__(self,
                   value: Any, /) -> bool:
            return not self.__eq__(value)

        @abc.abstractmethod
        def _c_eq(self,
                  value: Any, /) -> bool:
            """
            Called in __eq__ only if check of 'self is value' returned False.
            Note: __eq__ asserts for hash-consistency
            (i.e., if __debug__ is True, both self and value are hashable and they are equal w.r.t. _c_eq,
            their hash must also equal).
            """
            ...

    class TotallyOrdered(SupportsEquals,
                         Pr.TotallyOrdered[TYPE_contra],
                         abc.ABC):

        @total_ordering
        class __TotallyOrderedProxy(Generic[TYPE2_contra]):
            """
            Required since '@total_ordering' not supported by mypy (as of early 2020).
            """

            def __init__(self,
                         tot_ord: Pr.TotallyOrdered[TYPE2_contra]):
                self.__f_tot_ord: Final[Pr.TotallyOrdered[TYPE2_contra]] = tot_ord

            @final
            def __eq__(self,
                       value: Any, /) -> bool:
                return self.__f_tot_ord.__eq__(value)

            @final
            def __lt__(self,
                       value: TYPE2_contra, /) -> bool:
                return self.__f_tot_ord.__lt__(value)

        def __init__(self,
                     *args: Any,
                     **kwargs: Any):
            self.__f_tot_ord_proxy: Final[Ab.TotallyOrdered.__TotallyOrderedProxy[TYPE_contra]] = \
                Ab.TotallyOrdered.__TotallyOrderedProxy(tot_ord=self)
            super().__init__(*args, **kwargs)  # type: ignore

        @final
        def __le__(self,
                   value: TYPE_contra, /) -> bool:
            # mypy does not recognise @total_ordering decorator
            return cast(bool, self.__f_tot_ord_proxy.__le__(value))  # type: ignore

        @final
        def __gt__(self,
                   value: TYPE_contra, /) -> bool:
            # mypy does not recognise @total_ordering decorator
            return cast(bool, self.__f_tot_ord_proxy.__gt__(value))  # type: ignore

        @final
        def __ge__(self,
                   value: TYPE_contra, /) -> bool:
            # mypy does not recognise @total_ordering decorator
            return cast(bool, self.__f_tot_ord_proxy.__ge__(value))  # type: ignore


# Unmaintained
# class Im:
#     class DummyContextManager(Pr.ContextManager[TYPE_co]):
#
#         def __init__(self,
#                      enter_result: TYPE_co):
#             self.__f_enter_result: Final[TYPE_co] = enter_result
#
#         @final
#         def __enter__(self, /) -> TYPE_co:
#             return self.__f_enter_result
#
#         @final
#         def __exit__(self,
#                      exc_type: Optional[Type[BaseException]],
#                      exc_value: Optional[BaseException],
#                      exc_traceback: Optional[TracebackType], /) -> Literal[False]:
#             return False
