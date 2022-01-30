# con
from __future__ import annotations

import abc
import math
import queue
import time
from collections import deque
from typing import runtime_checkable, Protocol, Any, overload, Union, Optional, Tuple, final, \
    Final, Deque
import typing as ty

import utils.types.magicmethods as mm
from utils.types.typevars import TYPE_co, TYPE, SELF, TYPE2, V_TYPE_co, K_TYPE, V_TYPE

"""
Note: Unless explicitly stated otherwise, the following holds true:
Abstract implementations here aim at being suitable to be used as superclasses for concurrency safe 
implementations without the former having to overwrite any of the already implemented methods.
"""


class Ca:

    @staticmethod
    def _c_to_iter(obj: Pr.RandomAccess[TYPE]) -> Pr.Iterator[TYPE]:
        """
        The returned iterator accesses entries via an index which is incremented
        by one after each step. It therefore reflects subsequent changes to entries
        not yet reached. Returns when an IndexError occurs (which is suppressed).

        Note: The returned iterator itself is NOT concurrency safe!
        """
        index: int = 0
        value: TYPE
        try:
            while True:
                value = obj[index]
                yield value
                index += 1
        except IndexError:
            return

    @staticmethod
    def c_to_iter(obj: Union[Pr.RandomAccess[TYPE],
                             Pr.Iterable[TYPE]]) -> Pr.Iterator[TYPE]:
        if isinstance(obj, Pr.Iterable):
            return iter(obj)
        else:
            return Ca._c_to_iter(obj=obj)

    @staticmethod
    def c_to_itbl(obj: Union[Pr.RandomAccess[TYPE],
                             Pr.Iterable[TYPE]]) -> Pr.Iterable[TYPE]:
        if isinstance(obj, Pr.Iterable):
            return obj
        else:
            return Im.RandomAccessIterableWrapper(ra=obj)

    @staticmethod
    def c_to_ra(obj: Union[Pr.RandomAccess[TYPE],
                           Pr.Iterable[TYPE]]) -> Pr.RandomAccess[TYPE]:
        if isinstance(obj, Pr.RandomAccess):
            return obj
        else:
            assert isinstance(obj, Pr.Iterable)
            return tuple(obj)

    @staticmethod
    def c_to_mra(obj: Union[Pr.RandomAccess[TYPE],
                            Pr.Iterable[TYPE]]) -> Pr.MutableRandomAccess[TYPE]:
        if isinstance(obj, Pr.MutableRandomAccess):
            return obj
        else:
            return list(Ca.c_to_iter(obj=obj))

    @staticmethod
    def c_to_mlra(obj: Union[Pr.RandomAccess[TYPE],
                             Pr.Iterable[TYPE]]) -> Pr.MutableLengthRandomAccess[TYPE]:
        if isinstance(obj, Pr.MutableLengthRandomAccess):
            return obj
        else:
            return list(Ca.c_to_iter(obj=obj))

    @staticmethod
    def c_to_seq(obj: Union[Pr.RandomAccess[TYPE],
                            Pr.Iterable[TYPE]]) -> Pr.Sequence[TYPE]:
        if isinstance(obj, Pr.Sequence):
            return obj
        else:
            return tuple(Ca.c_to_iter(obj=obj))

    @staticmethod
    def c_to_mseq(obj: Union[Pr.RandomAccess[TYPE],
                             Pr.Iterable[TYPE]]) -> Pr.MutableSequence[TYPE]:
        if isinstance(obj, Pr.MutableSequence):
            return obj
        else:
            return list(Ca.c_to_iter(obj=obj))

    @staticmethod
    def c_to_mlseq(obj: Union[Pr.RandomAccess[TYPE],
                              Pr.Iterable[TYPE]]) -> Pr.MutableLengthSequence[TYPE]:
        if isinstance(obj, Pr.MutableLengthSequence):
            return obj
        else:
            return list(Ca.c_to_iter(obj=obj))

    @staticmethod
    def c_check_bounds(idx: int,
                       length: int) -> bool:
        if length < 0:
            raise ValueError

        return -length <= idx < length

    @staticmethod
    def c_get_num(idx: slice,
                  length: int) -> int:

        sl: Tuple[int, int, int] = idx.indices(length)
        assert sl[2] != 0
        if sl[0] == sl[1] or (sl[0] < sl[1]) == (sl[2] < 0):
            # slice(2,2,1) -> return 0
            # or slice(1,2,-1) -> return 0
            # or slice(2,1,1) -> return 0
            return 0
        else:
            if sl[0] > sl[1]:
                assert sl[2] < 0
                sl = (sl[1], sl[0], -sl[2])

            assert sl[0] < sl[1] and sl[2] > 0

            # result is 1 + n with n being the maximal integer n, s.th.: s[0] + n * s[2] <= s[1] - 1
            return 1 + math.floor((sl[1] - sl[0] - 1) / sl[2])

    @staticmethod
    def c_reversed(ra: Pr.RandomAccess[TYPE],
                   _force_custom_implementation: bool = False) -> Pr.Iterator[TYPE]:
        """
        Returns reversed(ra), if ra is an instance of Reversible.

        Otherwise, the returned iterator starts at the last element at the time of creation and
        accesses entries via an index which is decremented
        by one after each step. If the sequence gets shortened, missing entries are simply skipped.
        It therefore reflects subsequent changes to entries
        not yet reached.

        Note: The returned iterator itself is NOT concurrency safe!
        """

        if not _force_custom_implementation and isinstance(ra, Pr.Reversible):
            return reversed(ra)
        else:
            index = len(ra) - 1
            success: bool
            value: TYPE
            while index >= 0:
                success = False
                while not success and index >= 0:
                    try:
                        value = ra[index]
                        yield value
                        success = True
                    except IndexError:
                        index -= 1
                index -= 1
            return


class Pr:

    @runtime_checkable
    class Container(Protocol[TYPE_co]):

        @abc.abstractmethod
        def __contains__(self,
                         item: Any, /) -> bool:
            ...

    @runtime_checkable
    class Iterable(Protocol[TYPE_co]):

        @abc.abstractmethod
        def __iter__(self, /) -> Pr.Iterator[TYPE_co]:
            ...

    @runtime_checkable
    class Iterator(Iterable[TYPE_co],
                   Protocol[TYPE_co]):

        @abc.abstractmethod
        def __next__(self, /) -> TYPE_co:
            ...

    @runtime_checkable
    class Reversible(Iterable[TYPE_co],
                     Protocol[TYPE_co]):
        """
        Implies an ordering.
        """

        @abc.abstractmethod
        def __reversed__(self, /) -> Pr.Iterator[TYPE_co]:
            ...

    @runtime_checkable
    class Sized(Protocol):

        @abc.abstractmethod
        def __len__(self, /) -> int:
            ...

    @runtime_checkable
    class SizedIterable(Iterable[TYPE_co],
                        Sized,
                        Protocol[TYPE_co]):
        ...

    @runtime_checkable
    class RandomAccess(Sized,
                       Protocol[TYPE_co]):

        # noinspection PyProtocol
        @overload
        def __getitem__(self,
                        index: int, /) -> TYPE_co:
            ...

        # noinspection PyProtocol
        @overload
        def __getitem__(self,
                        index: slice, /) -> Pr.RandomAccess[TYPE_co]:
            ...

        @abc.abstractmethod
        def __getitem__(self,
                        index: Union[int, slice], /) -> Union[TYPE_co, Pr.RandomAccess[TYPE_co]]:
            ...

    @runtime_checkable
    class MutableRandomAccess(RandomAccess[TYPE],
                              Protocol[TYPE]):

        # noinspection PyProtocol
        @overload
        def __setitem__(self,
                        index: int,
                        value: TYPE, /) -> None:
            ...

        # noinspection PyProtocol
        @overload
        def __setitem__(self,
                        index: slice,
                        value: Pr.Iterable[TYPE], /) -> None:
            ...

        @abc.abstractmethod
        def __setitem__(self,
                        index: Union[int, slice],
                        value: Union[TYPE, Pr.Iterable[TYPE]], /) -> None:
            ...

    @runtime_checkable
    class MutableLengthRandomAccess(MutableRandomAccess[TYPE],
                                    Protocol[TYPE]):

        # noinspection PyProtocol
        @overload
        def __delitem__(self,
                        index: int, /) -> None:
            ...

        # noinspection PyProtocol
        @overload
        def __delitem__(self,
                        index: slice, /) -> None:
            ...

        @abc.abstractmethod
        def __delitem__(self,
                        index: Union[int, slice], /) -> None:
            ...

        @abc.abstractmethod
        def insert(self,
                   index: int,
                   value: TYPE, /) -> None:
            ...

    @runtime_checkable
    class Sequence(RandomAccess[TYPE_co],
                   Container[TYPE_co],
                   Iterable[TYPE_co],
                   Protocol[TYPE_co]):
        """
        Compatible with list and tuple.

        Note: Does not inherit from Reversible in order to make it a structural
        superclass of tuple (which, unfortunately, has no __reversed__ (yet still can be
        reversed via reversed(...)).
        """

        # noinspection PyProtocol
        @overload
        def __getitem__(self,
                        index: int, /) -> TYPE_co:
            ...

        # noinspection PyProtocol
        @overload
        def __getitem__(self,
                        index: slice, /) -> Pr.Sequence[TYPE_co]:
            ...

        @abc.abstractmethod
        def __getitem__(self,  # type: ignore
                        index: Union[int, slice], /) -> Union[TYPE_co, Pr.Sequence[TYPE_co]]:
            ...

        @abc.abstractmethod
        def index(self,
                  value: Any,
                  start: int = ...,
                  stop: int = ..., /) -> int:
            ...

        @abc.abstractmethod
        def count(self,
                  value: Any, /) -> int:
            ...

    @runtime_checkable
    class MutableSequence(MutableRandomAccess[TYPE],
                          Reversible[TYPE],
                          Sequence[TYPE],
                          Protocol[TYPE]):
        ...

    @runtime_checkable
    class MutableLengthSequence(MutableLengthRandomAccess[TYPE],
                                MutableSequence[TYPE],
                                Protocol[TYPE]):
        """
        Compatible with list.
        """

        @abc.abstractmethod
        def append(self,
                   value: TYPE, /) -> None:
            ...

        @abc.abstractmethod
        def clear(self, /) -> None:
            ...

        @abc.abstractmethod
        def extend(self,
                   values: Pr.Iterable[TYPE], /) -> None:
            ...

        @abc.abstractmethod
        def pop(self,
                index: int = ..., /) -> TYPE:
            ...

        @abc.abstractmethod
        def remove(self,
                   value: TYPE, /) -> None:
            ...

        @abc.abstractmethod
        def __iadd__(self: SELF,
                     values: Pr.Iterable[TYPE], /) -> SELF:
            ...

    @runtime_checkable
    class Set(Sized,
              Iterable[TYPE_co],
              Container[TYPE_co],
              mm.Pr.TotallyOrdered[ty.AbstractSet[Any]],
              Protocol[TYPE_co]):
        """
        Unfortunately, we ave to use ty.AbstractSet as parameter type in methods
        in order to ensure compatibility with typeshed's builtin set type
        (methods are contravariant in their parameters).
        """

        @abc.abstractmethod
        def __and__(self,
                    other: ty.AbstractSet[Any], /) -> Pr.Set[TYPE_co]:
            ...

        @abc.abstractmethod
        def __or__(self,
                   other: ty.AbstractSet[TYPE2], /) -> Pr.Set[Union[TYPE_co, TYPE2]]:
            ...

        @abc.abstractmethod
        def __sub__(self,
                    other: ty.AbstractSet[Any], /) -> Pr.Set[TYPE_co]:
            ...

        @abc.abstractmethod
        def __xor__(self,
                    other: ty.AbstractSet[TYPE2], /) -> Pr.Set[Union[TYPE_co, TYPE2]]:
            ...

        @abc.abstractmethod
        def isdisjoint(self,
                       other: Pr.Iterable[Any], /) -> bool:
            ...

    @runtime_checkable
    class MutableSet(Set[TYPE],
                     Protocol[TYPE]):

        @abc.abstractmethod
        def __and__(self,
                    other: ty.AbstractSet[Any], /) -> Pr.MutableSet[TYPE]:
            ...

        @abc.abstractmethod
        def __or__(self,
                   other: ty.AbstractSet[TYPE2], /) -> Pr.MutableSet[Union[TYPE, TYPE2]]:
            ...

        @abc.abstractmethod
        def __sub__(self,
                    other: ty.AbstractSet[Any], /) -> Pr.MutableSet[TYPE]:
            ...

        @abc.abstractmethod
        def __xor__(self,
                    other: ty.AbstractSet[TYPE2], /) -> Pr.MutableSet[Union[TYPE, TYPE2]]:
            ...

        @abc.abstractmethod
        def add(self,
                value: TYPE, /) -> None:
            ...

        @abc.abstractmethod
        def discard(self,
                    value: TYPE, /) -> None:
            ...

        @abc.abstractmethod
        def remove(self,
                   value: TYPE, /) -> None:
            """
            Same as discard but raises KeyError if value not present in set.
            """
            ...

        @abc.abstractmethod
        def pop(self, /) -> TYPE:
            ...

        @abc.abstractmethod
        def clear(self, /) -> None:
            ...

        @abc.abstractmethod
        def __ior__(self,
                    other: ty.AbstractSet[TYPE2], /) -> Pr.MutableSet[Union[TYPE, TYPE2]]:
            ...

        @abc.abstractmethod
        def __iand__(self,
                     other: ty.AbstractSet[Any], /) -> Pr.MutableSet[TYPE]:
            ...

        @abc.abstractmethod
        def __ixor__(self,
                     other: ty.AbstractSet[TYPE2], /) -> Pr.MutableSet[Union[TYPE, TYPE2]]:
            ...

        @abc.abstractmethod
        def __isub__(self,
                     other: ty.AbstractSet[Any], /) -> Pr.MutableSet[TYPE]:
            ...

    @runtime_checkable
    class ValuesView(Sized,
                     Iterable[V_TYPE_co],
                     Container[V_TYPE_co],
                     Protocol[V_TYPE_co]):
        """
        Values do not have to be hashable which is why Mapping.values() may not simply return a set.
        """
        ...

    @runtime_checkable
    class Mapping(Container[K_TYPE],
                  Sized,
                  Iterable[K_TYPE],
                  Protocol[K_TYPE, V_TYPE_co]):

        @abc.abstractmethod
        def __getitem__(self,
                        key: K_TYPE, /) -> V_TYPE_co:
            ...

        @overload
        def get(self,
                key: K_TYPE, /) -> Optional[V_TYPE_co]:
            ...

        @overload
        def get(self,
                key: K_TYPE,
                default: TYPE, /) -> Union[V_TYPE_co, TYPE]:
            ...

        @abc.abstractmethod
        def get(self,
                key: K_TYPE,
                default: Optional[TYPE] = ..., /) -> Union[V_TYPE_co, Optional[TYPE]]:
            ...

        @abc.abstractmethod
        def keys(self, /) -> Pr.Set[K_TYPE]:
            ...

        @abc.abstractmethod
        def items(self, /) -> Pr.Set[Tuple[K_TYPE, V_TYPE_co]]:
            ...

        @abc.abstractmethod
        def values(self, /) -> Pr.ValuesView[V_TYPE_co]:
            ...

    @runtime_checkable
    class MutableMapping(Mapping[K_TYPE, V_TYPE],
                         Protocol[K_TYPE, V_TYPE]):

        @abc.abstractmethod
        def __setitem__(self,
                        key: K_TYPE,
                        value: V_TYPE, /) -> None:
            ...

        @abc.abstractmethod
        def __delitem__(self,
                        key: K_TYPE, /) -> None:
            ...

        @abc.abstractmethod
        def clear(self, /) -> None:
            ...

        @overload
        def pop(self,
                key: K_TYPE, /) -> V_TYPE:
            """
            No default => Must return value (or raise KeyError if no value present).
            """
            ...

        @overload
        def pop(self,
                key: K_TYPE,
                default: TYPE, /) -> Union[V_TYPE, TYPE]:
            ...

        @abc.abstractmethod
        def pop(self,
                key: K_TYPE,
                default: Optional[TYPE] = ..., /) -> Union[V_TYPE, Optional[TYPE]]:
            ...

        @abc.abstractmethod
        def popitem(self, /) -> Tuple[K_TYPE, V_TYPE]:
            ...

        @abc.abstractmethod
        def setdefault(self,
                       key: K_TYPE,
                       default: V_TYPE, /) -> V_TYPE:
            ...

        # noinspection PyProtocol
        @overload
        def update(self,
                   other: ty.Mapping[K_TYPE, V_TYPE],
                   /,
                   **kwargs: V_TYPE) -> None:
            """
            Unfortunately, type of 'other' must be typing.Mapping instead of Pr.Mapping
            in order to be compatible with the former.

            Currently there is not way to ensure that kwargs is only provided if key is string.
            """
            ...

        @overload
        def update(self,
                   other: Pr.Iterable[Tuple[K_TYPE, V_TYPE]],
                   /,
                   **kwargs: V_TYPE) -> None:
            ...

        @overload
        def update(self,
                   /,
                   **kwargs: V_TYPE) -> None:
            ...

        @abc.abstractmethod
        def update(self,
                   other: Optional[Union[ty.Mapping[K_TYPE, V_TYPE],
                                         Pr.Iterable[Tuple[K_TYPE, V_TYPE]]]] = ...,
                   /,
                   **kwargs: V_TYPE) -> None:
            ...

    @runtime_checkable
    class Queue(Protocol[TYPE]):

        @abc.abstractmethod
        def qsize(self, /) -> int:
            ...

        @abc.abstractmethod
        def empty(self, /) -> bool:
            ...

        @abc.abstractmethod
        def full(self, /) -> bool:
            ...

        @abc.abstractmethod
        def put(self,
                item: TYPE,
                block: bool = ...,
                timeout: Optional[float] = ..., /) -> None:
            """
            Raises queue.Full exception if not successful.

            Unfortunately, builtin queues use 'block' as parameter name
            instead of 'blocking' (which would have been consistent with lock methods).
            """
            ...

        @abc.abstractmethod
        def put_nowait(self,
                       item: TYPE, /) -> None:
            """
            Equivalent to put(item, False).
            """
            ...

        @abc.abstractmethod
        def get(self,
                block: bool = ...,
                timeout: Optional[float] = ..., /) -> TYPE:
            """
            Raises queue.Empty exception if not successful.

            Unfortunately, builtin queues use 'block' as parameter name
            instead of 'blocking' (which would have been consistent with lock methods).
            """
            ...

        @abc.abstractmethod
        def get_nowait(self, /) -> TYPE:
            """
            Equivalent to get(False).
            """
            ...

    @runtime_checkable
    class CapacityQueue(Queue[TYPE],
                        Protocol[TYPE]):

        @abc.abstractmethod
        def c_capacity(self, /) -> Optional[int]:
            ...


# Unfortunately, tuple has no __reversed__ method. Is is, however, still reversible
# via reversed(). PyCharm falsely claims that reference to register is unresolved,
# therefore the noinspection clause.
# noinspection PyUnresolvedReferences
Pr.Reversible.register(tuple)


class Ab:

    class Iterator(Pr.Iterator[TYPE_co],
                   abc.ABC):

        @final
        def __iter__(self, /) -> Pr.Iterator[TYPE_co]:
            return self

    class RandomAccessIterable(Pr.RandomAccess[TYPE_co],
                               Pr.Iterable[TYPE_co],
                               abc.ABC):
        @final
        def __iter__(self, /) -> Pr.Iterator[TYPE_co]:
            # noinspection PyProtectedMember
            return Ca._c_to_iter(obj=self)

    class Sequence(mm.Ab.SupportsEquals,
                   RandomAccessIterable[TYPE_co],
                   Pr.Sequence[TYPE_co],
                   abc.ABC):
        ...

    class MutableSequence(Sequence[TYPE],
                          Pr.MutableSequence[TYPE],
                          abc.ABC):

        @final
        def __reversed__(self, /) -> Pr.Iterator[TYPE]:
            return Ca.c_reversed(ra=self,
                                 _force_custom_implementation=True)

    class MutableLengthSequence(MutableSequence[TYPE],
                                Pr.MutableLengthSequence[TYPE],
                                abc.ABC):
        pass

    class Mapping(mm.Ab.SupportsEquals,
                  Pr.Mapping[K_TYPE, V_TYPE_co],
                  abc.ABC):

        @final
        def __len__(self, /) -> int:
            return len(self.keys())

        # noinspection PyMethodOverriding
        @overload
        def get(self,
                key: K_TYPE, /) -> Optional[V_TYPE_co]:
            ...

        # noinspection PyMethodOverriding
        @overload
        def get(self,
                key: K_TYPE,
                default: TYPE, /) -> Union[V_TYPE_co, TYPE]:
            ...

        @final
        def get(self,  # type: ignore
                key: K_TYPE,
                default: Optional[TYPE] = None, /) -> Union[V_TYPE_co, Optional[TYPE]]:
            try:
                return self[key]
            except KeyError:
                return default

        @final
        def __contains__(self,
                         key: Any, /) -> bool:
            return key in self.keys()

        @final
        def __iter__(self, /) -> Pr.Iterator[K_TYPE]:
            return iter(self.keys())

    class MutableMapping(Mapping[K_TYPE, V_TYPE],
                         Pr.MutableMapping[K_TYPE, V_TYPE],
                         abc.ABC):
        pass

    class CapacityQueue(Pr.CapacityQueue[TYPE],
                        Pr.Sized,  # Note: manager.Queue does not support Sized
                        abc.ABC):
        __f_capacity: Final[Optional[int]]

        def __init__(self,
                     capacity: Optional[int] = None):
            """
            :param capacity: None represents an unbound queue.
            """
            if capacity is not None and capacity < 0:
                raise ValueError(f"Negative capacity provided: {capacity}")

            self.__f_capacity = capacity

        @final
        def c_capacity(self, /) -> Optional[int]:
            return self.__f_capacity

        @final
        def empty(self, /) -> bool:
            return self.qsize() == 0

        @final
        def full(self, /) -> bool:
            if self.__f_capacity is None:
                return False
            else:
                return self.qsize() == self.__f_capacity

        @final
        def put_nowait(self,
                       item: TYPE, /) -> None:
            self.put(item, False)

        @final
        def get_nowait(self, /) -> TYPE:
            return self.get(False)

        @final
        def __len__(self, /) -> int:
            return self.qsize()


class Im:
    class DequeQueue(Ab.CapacityQueue[TYPE]):
        """
        NON concurrency safe CapacityQueue backed by a deque. Raises RuntimeError
        if put/get called with block = True and infinite timeout (which would result in a
        deadlock in a Single Thread environment (for which this class is intended)).
        A blocking put/get call with block = True and finite timeout results in
        this class sleeping for timeout amount of time and then raising Full/Empty.
        This is behavior is consistent with the one of queue.Queue.
        """

        __f_deque: Deque[TYPE]

        def __init__(self,
                     capacity: Optional[int] = None):
            super().__init__(capacity=capacity)
            self.__f_deque = deque(maxlen=None)  # Capacity checks performed here

        @final
        def put(self,
                item: TYPE,
                block: bool = True,
                timeout: Optional[float] = None, /) -> None:

            if self.full():
                if block:
                    if timeout is None:
                        raise RuntimeError("Deadlock: Attempting to perform operation which would lead to "
                                           "unbound blocking in a single thread context.")
                    else:
                        time.sleep(timeout)

                raise queue.Full()
            else:
                self.__f_deque.appendleft(item)

        @final
        def get(self,
                block: bool = True,
                timeout: Optional[float] = None, /) -> TYPE:
            if self.empty():
                if block:
                    if timeout is None:
                        raise RuntimeError("Deadlock: Attempting to perform operation leading to "
                                           "unbound blocking in a single thread context.")
                    else:
                        time.sleep(timeout)
                raise queue.Empty()
            else:
                return self.__f_deque.pop()

        @final
        def qsize(self, /) -> int:
            return len(self.__f_deque)

    class RandomAccessIterableWrapper(Ab.RandomAccessIterable[TYPE_co]):

        def __init__(self,
                     ra: Pr.RandomAccess[TYPE_co]):
            self.__f_ra: Final[Pr.RandomAccess[TYPE_co]] = ra

        @overload
        def __getitem__(self,
                        index: int, /) -> TYPE_co:
            ...

        @overload
        def __getitem__(self,
                        index: slice, /) -> Pr.RandomAccess[TYPE_co]:
            ...

        @final
        def __getitem__(self,  # type: ignore
                        index: Union[int, slice], /) -> Union[TYPE_co, Pr.RandomAccess[TYPE_co]]:
            return self.__f_ra[index]

        @final
        def __len__(self, /) -> int:
            return len(self.__f_ra)
