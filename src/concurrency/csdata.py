from __future__ import annotations

import abc
from typing import Protocol, Tuple, Callable, Optional, Union, overload, runtime_checkable, Any, Final, final, cast
import multiprocessing.managers as mp_mngr

import utils.types.containers as con
from concurrency import csrwlocks, cs
from utils.types.typevars import TYPE, TYPE2, SELF
import concurrency._raw_csdata as rcsd


class Pr:

    @runtime_checkable
    class CSData(csrwlocks.Pr.CSRWLockableMixin,
                 Protocol[TYPE]):

        @abc.abstractmethod
        def c_apply(self,
                    func: Callable[[TYPE], Tuple[TYPE, TYPE2]],
                    blocking: bool = ...,
                    timeout: Optional[float] = ...,
                    _unsafe: bool = ...) -> TYPE2:
            """
            func is called on internal value. First entry of returned tuple is assigned to internal value
            if it differs.
            Second tuple entry is returned.
            """
            ...

        @abc.abstractmethod
        def c_get(self,
                  blocking: bool = ...,
                  timeout: Optional[float] = ...,
                  _unsafe: bool = ...) -> TYPE:
            ...

        @abc.abstractmethod
        def c_set(self,
                  new_val: TYPE,
                  blocking: bool = ...,
                  timeout: Optional[float] = ...,
                  _unsafe: bool = ...) -> None:
            ...

    @runtime_checkable
    class CSMutableSequence(csrwlocks.Pr.CSRWLockableMixin,
                            con.Pr.MutableSequence[TYPE],
                            Protocol[TYPE]):

        @overload
        def c_apply(self,
                    index: int,
                    func: Callable[[TYPE], Tuple[TYPE, TYPE2]],
                    blocking: bool = ...,
                    timeout: Optional[float] = ...,
                    _unsafe: bool = ...) -> TYPE2:
            ...

        @overload
        def c_apply(self,
                    index: slice,
                    func: Callable[[con.Pr.RandomAccess[TYPE]], Tuple[con.Pr.Iterable[TYPE], TYPE2]],
                    blocking: bool = ...,
                    timeout: Optional[float] = ...,
                    _unsafe: bool = ...) -> TYPE2:
            ...

        @abc.abstractmethod
        def c_apply(self,
                    index: Union[int, slice],
                    func: Union[Callable[[TYPE], Tuple[TYPE, TYPE2]],
                                Callable[[con.Pr.RandomAccess[TYPE]], Tuple[con.Pr.Iterable[TYPE], TYPE2]]],
                    blocking: bool = ...,
                    timeout: Optional[float] = ...,
                    _unsafe: bool = ...) -> TYPE2:
            """
            func is called on internal values indexed by slice.
            First entry of returned tuple is an Iterable whose entries are assigned to positions indexed by
            slice in the order they appear in said iterable. Note that the number of elements in said iterable must
            equal the number of entries indexed by slice (otherwise IndexError is raised).
            Second tuple entry is returned.
            Note: When defining 'func', you may use utils.types.containers.Ca.c_to_itbl to get an
            iterable view on the random access provided to 'func'.
            """
            ...

        # noinspection PyProtocol
        @overload
        def c_get(self,
                  index: int,
                  blocking: bool = ...,
                  timeout: Optional[float] = ...,
                  _unsafe: bool = ...) -> TYPE:
            ...

        # noinspection PyProtocol
        @overload
        def c_get(self,
                  index: slice,
                  blocking: bool = ...,
                  timeout: Optional[float] = ...,
                  _unsafe: bool = ...) -> con.Pr.Sequence[TYPE]:
            ...

        @abc.abstractmethod
        def c_get(self,
                  index: Union[int, slice],
                  blocking: bool = ...,
                  timeout: Optional[float] = ...,
                  _unsafe: bool = ...) -> Union[TYPE, con.Pr.Sequence[TYPE]]:
            ...

        # noinspection PyProtocol
        @overload
        def c_set(self,
                  index: int,
                  value: TYPE,
                  blocking: bool = ...,
                  timeout: Optional[float] = ...,
                  _unsafe: bool = ...) -> None:
            ...

        # noinspection PyProtocol
        @overload
        def c_set(self,
                  index: slice,
                  value: con.Pr.Iterable[TYPE],
                  blocking: bool = ...,
                  timeout: Optional[float] = ...,
                  _unsafe: bool = ...) -> None:
            """
            Note: Length of iterable must equal to the number of entries indexed by slice.
            """
            ...

        @abc.abstractmethod
        def c_set(self,
                  index: Union[int, slice],
                  value: Union[TYPE, con.Pr.Iterable[TYPE]],
                  blocking: bool = ...,
                  timeout: Optional[float] = ...,
                  _unsafe: bool = ...) -> None:
            ...

        @abc.abstractmethod
        def c_len(self,
                  blocking: bool = ...,
                  timeout: Optional[float] = ...,
                  _unsafe: bool = ...) -> int:
            ...

    @runtime_checkable
    class CSMutableLengthSequence(con.Pr.MutableLengthSequence[TYPE],
                                  CSMutableSequence[TYPE],
                                  Protocol[TYPE]):

        @abc.abstractmethod
        def c_append(self,
                     value: TYPE,
                     blocking: bool = ...,
                     timeout: Optional[float] = ...,
                     _unsafe: bool = ...) -> None:
            ...

        @abc.abstractmethod
        def c_extend(self,
                     values: con.Pr.Iterable[TYPE],
                     blocking: bool = ...,
                     timeout: Optional[float] = ...,
                     _unsafe: bool = ...) -> None:
            ...

        # noinspection PyProtocol
        @overload
        def c_apply(self,
                    index: int,
                    func: Callable[[TYPE], Tuple[TYPE, TYPE2]],
                    blocking: bool = ...,
                    timeout: Optional[float] = ...,
                    _unsafe: bool = ...) -> TYPE2:
            ...

        # noinspection PyProtocol
        @overload
        def c_apply(self,
                    index: slice,
                    func: Callable[[con.Pr.RandomAccess[TYPE]], Tuple[con.Pr.Iterable[TYPE], TYPE2]],
                    blocking: bool = ...,
                    timeout: Optional[float] = ...,
                    _unsafe: bool = ...) -> TYPE2:
            ...

        @abc.abstractmethod
        def c_apply(self,
                    index: Union[int, slice],
                    func: Union[Callable[[TYPE], Tuple[TYPE, TYPE2]],
                                Callable[[con.Pr.RandomAccess[TYPE]], Tuple[con.Pr.Iterable[TYPE], TYPE2]]],
                    blocking: bool = ...,
                    timeout: Optional[float] = ...,
                    _unsafe: bool = ...) -> TYPE2:
            """
            func is called on internal values indexed by slice.
            First entry of returned tuple is an Iterable whose entries replace the positions indexed
            by slice. Note that this implies that the length of the underlying sequence might change
            (which is the main difference to CSMutableSequence which is more akin to an array of fixed size).
            Second tuple entry is returned.
            Note: When defining 'func', you may use utils.types.containers.Ca.c_to_itbl to get an
            iterable view on the random access provided to 'func'.
            """
            ...

        @abc.abstractmethod
        def c_insert(self,
                     index: int,
                     values: con.Pr.Iterable[TYPE],
                     blocking: bool = ...,
                     timeout: Optional[float] = ...,
                     _unsafe: bool = ...) -> None:
            """
            Result is roughly equivalent to pseudocode:
            self[index:(index + 1)] = values + self[index:(index + 1)]
            """
            ...

        @overload
        def c_delete(self,
                     index: int,
                     blocking: bool = ...,
                     timeout: Optional[float] = ...,
                     _unsafe: bool = ...) -> None:
            ...

        @overload
        def c_delete(self,
                     index: slice,
                     blocking: bool = ...,
                     timeout: Optional[float] = ...,
                     _unsafe: bool = ...) -> None:
            ...

        @abc.abstractmethod
        def c_delete(self,
                     index: Union[int, slice],
                     blocking: bool = ...,
                     timeout: Optional[float] = ...,
                     _unsafe: bool = ...) -> None:
            ...

    @runtime_checkable
    class CSCapacityQueue(cs.Pr.HasCSLMixin,
                          con.Pr.CapacityQueue[TYPE],
                          Protocol[TYPE]):

        @abc.abstractmethod
        def c_put(self,
                  item: TYPE,
                  blocking: bool = ...,
                  timeout: Optional[float] = ...) -> None:
            """
            Raises queue.Full exception if not successful.
            """
            ...

        @abc.abstractmethod
        def c_get(self,
                  blocking: bool = ...,
                  timeout: Optional[float] = ...) -> TYPE:
            """
            Raises queue.Empty exception if not successful.
            """
            ...

    @runtime_checkable
    class CSChunkCapacityQueue(CSCapacityQueue[Tuple[TYPE, ...]],
                               Protocol[TYPE]):
        """
        Meant for sending and receiving data as 'chunks' represented by tuples (which are immutable).
        """

        @abc.abstractmethod
        def c_get_min_chunk_size(self) -> Optional[int]:
            ...

        @abc.abstractmethod
        def c_get_max_chunk_size(self) -> Optional[int]:
            ...

    @runtime_checkable
    class CSCloseableMixin(csrwlocks.Pr.CSRWLockableMixin,
                           Protocol):

        @abc.abstractmethod
        def c_close(self,
                    blocking: bool = ...,
                    timeout: Optional[float] = ...,
                    _unsafe: bool = ...) -> bool:
            """
            Returns True iff closeable was not closed before.
            """
            ...

        @abc.abstractmethod
        def c_is_closed(self,
                        blocking: bool = ...,
                        timeout: Optional[float] = ...,
                        _unsafe: bool = ...) -> bool:
            ...


class Ab:

    class CSData(csrwlocks.Im.CSRWLockableMixin,
                 Pr.CSData[TYPE]):

        def __init__(self,
                     csl: cs.En.CSL,
                     initial_value: TYPE,
                     manager: Optional[mp_mngr.SyncManager] = None,
                     **kwargs: Any):
            self.__f_value: Final[rcsd.Pr.Value[TYPE]] = self._c_init_value(csl=csl,
                                                                            initial_value=initial_value,
                                                                            manager=manager,
                                                                            **kwargs)
            super().__init__(csl=csl,
                             manager=manager)

        @abc.abstractmethod
        def _c_init_value(self,
                          csl: cs.En.CSL,
                          initial_value: TYPE,
                          manager: Optional[mp_mngr.SyncManager] = None,
                          **kwargs: Any) -> rcsd.Pr.Value[TYPE]:
            ...

        @final
        @csrwlocks.De.d_cswlock
        def c_apply(self,
                    func: Callable[[TYPE], Tuple[TYPE, TYPE2]],
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> TYPE2:

            old_val: Final[TYPE] = self.__f_value.value
            result: Final[Tuple[TYPE, TYPE2]] = func(old_val)

            if result[0] != old_val:
                self.__f_value.value = result[0]

            return result[1]

        @final
        @csrwlocks.De.d_csrlock
        def c_get(self,
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> TYPE:
            return self.__f_value.value

        @final
        @csrwlocks.De.d_cswlock
        def c_set(self,
                  new_val: TYPE,
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> None:
            self.__f_value.value = new_val

    class CSMutableSequence(csrwlocks.Im.CSRWLockableMixin,
                            con.Ab.MutableSequence[TYPE],
                            Pr.CSMutableSequence[TYPE]):

        def __init__(self,
                     csl: cs.En.CSL,
                     initial_values: con.Pr.Iterable[TYPE],
                     manager: Optional[mp_mngr.SyncManager] = None,
                     **kwargs: Any):

            self.__f_mra: Final[con.Pr.MutableRandomAccess[TYPE]] = self._c_init_mra(
                csl=csl,
                initial_values=initial_values,
                manager=manager,
                **kwargs)

            super().__init__(csl=csl,
                             manager=manager)

        def _c_get_mra(self) -> con.Pr.MutableRandomAccess[TYPE]:
            """
            Use with caution! Only meant for internal use by subclasses!
            """
            return self.__f_mra

        @abc.abstractmethod
        def _c_init_mra(self,
                        csl: cs.En.CSL,
                        initial_values: con.Pr.Iterable[TYPE],
                        manager: Optional[mp_mngr.SyncManager] = None,
                        **kwargs: Any) -> con.Pr.MutableRandomAccess[TYPE]:
            ...

        @final
        def _c_eq(self, value: Any, /) -> bool:
            return self.__c_eq(value=value,
                               blocking=True,
                               timeout=None,
                               _unsafe=False)

        # noinspection PyUnusedLocal
        @final
        @csrwlocks.De.d_csrlock
        def __c_eq(self,
                   value: Any,
                   blocking: bool,
                   timeout: Optional[float],
                   _unsafe: bool) -> bool:

            """
            Note: This method is approximate in case 'value' got changed concurrently unless value
            is an instance of CSMutableSequence, in which case a csrlock of the former is acquired.
            """
            self_len: Final[int] = len(self.__f_mra)
            if isinstance(value, Pr.CSMutableSequence):
                with value.c_get_csrwlock().c_get_csrlock().c_acquire_timeout(blocking=True,
                                                                              timeout=None):

                    return (value.c_len(_unsafe=True) == self_len
                            and all(value.c_get(index=i, _unsafe=True) == self.__f_mra[i]
                                    for i in range(self_len)))

            elif isinstance(value, con.Pr.RandomAccess):
                if self_len != len(value):
                    return False
                else:
                    try:
                        return all(self.__f_mra[i] == value[i]
                                   for i in range(self_len))
                    except IndexError:
                        # Might happen if value is an instance of MutableLengthRandomAccess and
                        # has been changed concurrently.
                        return False
            else:
                return False

        @overload
        def __setitem__(self,
                        index: int,
                        value: TYPE, /) -> None:
            ...

        @overload
        def __setitem__(self,
                        index: slice,
                        value: con.Pr.Iterable[TYPE], /) -> None:
            ...

        @final
        def __setitem__(self,
                        index: Union[int, slice],
                        value: Union[TYPE, con.Pr.Iterable[TYPE]], /) -> None:
            if isinstance(index, int):
                # if-construct necessary for static typing
                value = cast(TYPE, value)
                self.c_set(index=index,
                           value=value,
                           blocking=True,
                           timeout=None)
            else:
                assert isinstance(index, slice)
                value = cast(con.Pr.Iterable[TYPE], value)
                self.c_set(index=index,
                           value=value,
                           blocking=True,
                           timeout=None)

        @overload
        def __getitem__(self,
                        index: int, /) -> TYPE:
            ...

        @overload
        def __getitem__(self,
                        index: slice, /) -> con.Pr.Sequence[TYPE]:
            ...

        @final
        def __getitem__(self,  # type: ignore
                        index: Union[int, slice], /) -> Union[TYPE, con.Pr.Sequence[TYPE]]:
            return self.c_get(index=index,
                              blocking=True,
                              timeout=None)

        @final
        def index(self,
                  value: Any,
                  start: int = 0,
                  stop: int = cast(int, None), /) -> int:
            return self.__index(value=value,
                                start=start,
                                stop=stop,
                                blocking=True,
                                timeout=None,
                                _unsafe=False)

        # noinspection PyUnusedLocal
        @final
        @csrwlocks.De.d_csrlock
        def __index(self,
                    value: Any,
                    start: int,
                    stop: int,
                    blocking: bool,
                    timeout: Optional[float],
                    _unsafe: bool) -> int:
            sl: Tuple[int, int, int] = slice(start, stop).indices(self.c_len(_unsafe=True))

            for idx in range(*sl):
                if self.__f_mra[idx] == value:
                    return idx

            raise ValueError(f"'{value}' not in subsequence.")

        @final
        def count(self, value: Any, /) -> int:
            return self.__count(value=value,
                                blocking=True,
                                timeout=None,
                                _unsafe=False)

        # noinspection PyUnusedLocal
        @final
        @csrwlocks.De.d_csrlock
        def __count(self,
                    value: Any,
                    blocking: bool,
                    timeout: Optional[float],
                    _unsafe: bool) -> int:
            count: int = 0
            for idx in range(self.c_len(_unsafe=True)):
                if self.__f_mra[idx] == value:
                    count += 1

            return count

        @final
        def __len__(self, /) -> int:
            return self.c_len(blocking=True,
                              timeout=None)

        @final
        def __contains__(self, item: Any, /) -> bool:
            try:
                self.index(item)
                return True
            except ValueError:
                return False

        # noinspection PyProtocol
        @overload
        def c_apply(self,
                    index: int,
                    func: Callable[[TYPE], Tuple[TYPE, TYPE2]],
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> TYPE2:
            ...

        # noinspection PyProtocol
        @overload
        def c_apply(self,
                    index: slice,
                    func: Callable[[con.Pr.RandomAccess[TYPE]], Tuple[con.Pr.Iterable[TYPE], TYPE2]],
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> TYPE2:
            ...

        @final
        @csrwlocks.De.d_cswlock
        def c_apply(self,
                    index: Union[int, slice],
                    func: Union[Callable[[TYPE], Tuple[TYPE, TYPE2]],
                                Callable[[con.Pr.RandomAccess[TYPE]], Tuple[con.Pr.Iterable[TYPE], TYPE2]]],
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> TYPE2:

            self_len: Final[int] = self.c_len(_unsafe=True)
            if isinstance(index, int):
                if not con.Ca.c_check_bounds(idx=index,
                                             length=self_len):
                    raise IndexError

                func = cast(Callable[[TYPE], Tuple[TYPE, TYPE2]], func)
                old_val: Final[TYPE] = self.__f_mra[index]
                res_1: Tuple[TYPE, TYPE2] = func(old_val)

                if res_1[0] != old_val:
                    self.__f_mra[index] = res_1[0]

                return res_1[1]
            else:
                assert isinstance(index, slice)
                func = cast(Callable[[con.Pr.RandomAccess[TYPE]], Tuple[con.Pr.Iterable[TYPE], TYPE2]], func)
                old_vals: con.Pr.RandomAccess[TYPE] = self.__f_mra[index]
                res_2: Tuple[con.Pr.Iterable[TYPE], TYPE2] = func(old_vals)

                # We must make sure that the new_vals are pickable (e.g., are not a generator)
                # Although technically, a Sequence does not have to be pickable, most low level sequence
                # implementations are (tuple, list, ...). Otherwise, the corresponding manager
                # will raise an Exception and the user must make sure that 'func'
                # returns a pickable Sequence (note that c_to_seq only converts, if
                # provided argument is not already a sequence).
                new_vals: con.Pr.Sequence[TYPE] = con.Ca.c_to_seq(obj=res_2[0])

                if not isinstance(self, con.Pr.MutableLengthRandomAccess):
                    len_old_vals: Final[int] = len(old_vals)
                    if len(new_vals) != len_old_vals:
                        raise IndexError("Number of new values to be assigned does not "
                                         "equal number of indexed positions.")

                    if any(new_vals[i] != old_vals[i]
                           for i in range(len_old_vals)):
                        self.__f_mra[index] = new_vals
                else:
                    self.__f_mra[index] = new_vals

                return res_2[1]

        # noinspection PyProtocol
        @overload
        def c_get(self,
                  index: int,
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> TYPE:
            ...

        # noinspection PyProtocol
        @overload
        def c_get(self,
                  index: slice,
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> con.Pr.Sequence[TYPE]:
            ...

        @final
        @csrwlocks.De.d_csrlock
        def c_get(self,  # type: ignore
                  index: Union[int, slice],
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> Union[TYPE, con.Pr.Sequence[TYPE]]:
            if isinstance(index, int):
                return self.__f_mra[index]
            else:
                return con.Ca.c_to_seq(self.__f_mra[index])

        # noinspection PyProtocol
        @overload
        def c_set(self,
                  index: int,
                  value: TYPE,
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> None:
            ...

        # noinspection PyProtocol
        @overload
        def c_set(self,
                  index: slice,
                  value: con.Pr.Iterable[TYPE],
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> None:
            ...

        @final
        def c_set(self,
                  index: Union[int, slice],
                  value: Union[TYPE, con.Pr.Iterable[TYPE]],
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> None:
            if isinstance(index, int):
                value = cast(TYPE, value)
                self.c_apply(index=index,
                             func=lambda old_val: (value, None),
                             blocking=blocking,
                             timeout=timeout,
                             _unsafe=_unsafe)
            else:
                assert isinstance(index, slice)
                value = cast(con.Pr.Iterable[TYPE], value)
                self.c_apply(index=index,
                             func=lambda old_val: (value, None),
                             blocking=blocking,
                             timeout=timeout,
                             _unsafe=_unsafe)

        @final
        @csrwlocks.De.d_csrlock
        def c_len(self,
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> int:
            return len(self.__f_mra)

    class CSMutableLengthSequence(CSMutableSequence[TYPE],
                                  Pr.CSMutableLengthSequence[TYPE]):

        @abc.abstractmethod
        def _c_init_mra(self,
                        csl: cs.En.CSL,
                        initial_values: con.Pr.Iterable[TYPE],
                        manager: Optional[mp_mngr.SyncManager] = None,
                        **kwargs: Any) -> con.Pr.MutableLengthRandomAccess[TYPE]:
            ...

        @final
        def _c_get_mra(self) -> con.Pr.MutableLengthRandomAccess[TYPE]:
            result: con.Pr.MutableRandomAccess[TYPE] = super()._c_get_mra()
            assert isinstance(result, con.Pr.MutableLengthRandomAccess)
            return result

        @overload
        def __delitem__(self,
                        index: int, /) -> None:
            ...

        @overload
        def __delitem__(self,
                        index: slice, /) -> None:
            ...

        @final
        def __delitem__(self,
                        index: Union[int, slice], /) -> None:
            self.c_delete(index=index,
                          blocking=True,
                          timeout=None)

        @final
        def insert(self,
                   index: int,
                   value: TYPE, /) -> None:
            self.c_insert(index=index,
                          values=(value,),
                          blocking=True,
                          timeout=None)

        @final
        def c_insert(self,
                     index: int,
                     values: con.Pr.Iterable[TYPE],
                     blocking: bool = True,
                     timeout: Optional[float] = None,
                     _unsafe: bool = False) -> None:

            def func(old_vals: con.Pr.RandomAccess[TYPE]) -> Tuple[con.Pr.Iterable[TYPE], None]:
                assert len(old_vals) <= 1
                return (val
                        for itbl in (values, con.Ca.c_to_itbl(old_vals))
                        for val in itbl), None

            self.c_apply(index=slice(index, index+1),
                         func=func,
                         blocking=blocking,
                         timeout=timeout,
                         _unsafe=_unsafe)
            # Old, probably less efficient implementation (multiple send/receives on the unterlying mlra).
            # internal_mlra: con.Pr.MutableLengthRandomAccess[TYPE] = self._c_get_mra()
            #
            # for val in reversed(tuple(values)):
            #     internal_mlra.insert(index, val)

        # noinspection PyProtocol
        @overload
        def c_delete(self,
                     index: int,
                     blocking: bool = ...,
                     timeout: Optional[float] = ...,
                     _unsafe: bool = ...) -> None:
            ...

        # noinspection PyProtocol
        @overload
        def c_delete(self,
                     index: slice,
                     blocking: bool = ...,
                     timeout: Optional[float] = ...,
                     _unsafe: bool = ...) -> None:
            ...

        @final
        @csrwlocks.De.d_cswlock
        def c_delete(self,
                     index: Union[int, slice],
                     blocking: bool = True,
                     timeout: Optional[float] = None,
                     _unsafe: bool = False) -> None:

            internal_mlra: con.Pr.MutableLengthRandomAccess[TYPE] = self._c_get_mra()
            del internal_mlra[index]

        @final
        def append(self,
                   value: TYPE, /) -> None:
            self.extend((value,))

        @final
        @csrwlocks.De.d_cswlock
        def c_append(self,
                     value: TYPE,
                     blocking: bool = True,
                     timeout: Optional[float] = None,
                     _unsafe: bool = False) -> None:
            self.c_extend(values=(value,),
                          _unsafe=True)

        @final
        def clear(self, /) -> None:
            return self.__clear(blocking=True,
                                timeout=None,
                                _unsafe=False)

        # noinspection PyUnusedLocal
        @final
        @csrwlocks.De.d_cswlock
        def __clear(self,
                    blocking: bool,
                    timeout: Optional[float],
                    _unsafe: bool) -> None:
            del self._c_get_mra()[0:]

        @final
        def extend(self,
                   values: con.Pr.Iterable[TYPE], /) -> None:
            return self.__extend(values=values,
                                 blocking=True,
                                 timeout=None,
                                 _unsafe=False)

        @final
        @csrwlocks.De.d_cswlock
        def c_extend(self,
                     values: con.Pr.Iterable[TYPE],
                     blocking: bool = True,
                     timeout: Optional[float] = None,
                     _unsafe: bool = False) -> None:
            self.__extend(values=values,
                          blocking=False,
                          timeout=None,
                          _unsafe=True)

        # noinspection PyUnusedLocal
        @final
        @csrwlocks.De.d_cswlock
        def __extend(self,
                     values: con.Pr.Iterable[TYPE],
                     blocking: bool,
                     timeout: Optional[float],
                     _unsafe: bool) -> None:
            self.c_set(index=slice(self.c_len(_unsafe=True), None),
                       value=values,
                       _unsafe=True)

        @final
        def pop(self,
                index: int = -1, /) -> TYPE:
            return self.__pop(index=index,
                              blocking=True,
                              timeout=None,
                              _unsafe=False)

        # noinspection PyUnusedLocal
        @final
        @csrwlocks.De.d_cswlock
        def __pop(self,
                  index: int,
                  blocking: bool,
                  timeout: Optional[float],
                  _unsafe: bool) -> TYPE:

            self_len: Final[int] = self.c_len(_unsafe=True)

            if self_len == 0:
                raise IndexError

            if not con.Ca.c_check_bounds(idx=index,
                                         length=self_len):
                raise IndexError

            if index < 0:
                index += self_len

            assert 0 <= index < self_len

            internal_mlra: con.Pr.MutableLengthRandomAccess[TYPE] = self._c_get_mra()

            result: Final[TYPE] = internal_mlra[index]
            del internal_mlra[index]

            return result

        @final
        def remove(self,
                   value: TYPE, /) -> None:
            return self.__remove(value=value,
                                 blocking=True,
                                 timeout=None,
                                 _unsafe=False)

        # noinspection PyUnusedLocal
        @final
        @csrwlocks.De.d_cswlock
        def __remove(self,
                     value: TYPE,
                     blocking: bool,
                     timeout: Optional[float],
                     _unsafe: bool) -> None:
            mlra: con.Pr.MutableLengthRandomAccess[TYPE] = self._c_get_mra()
            for i in range(self.c_len(_unsafe=True)):
                if mlra[i] == value:
                    self.c_delete(index=i,
                                  _unsafe=True)
                    return
            raise ValueError  # 'value' not present

        @final
        def __iadd__(self: SELF,
                     values: con.Pr.Iterable[TYPE], /) -> SELF:
            if not isinstance(self, Pr.CSMutableLengthSequence):
                raise TypeError(f"'__iadd__' only supported for {Pr.CSMutableLengthSequence.__name__} "
                                f"instances.")
            self.extend(values)
            return cast(SELF, self)

    class CSCapacityQueue(cs.Im.HasCSLMixin,
                          con.Ab.CapacityQueue[TYPE],
                          Pr.CSCapacityQueue[TYPE]):

        def __init__(self,
                     csl: cs.En.CSL,
                     init_values: con.Pr.SizedIterable[TYPE],
                     capacity: Optional[int] = None,
                     manager: Optional[mp_mngr.SyncManager] = None,
                     **kwargs: Any):

            num_values: Final[int] = len(init_values)
            self.__f_queue: Final[con.Pr.Queue[TYPE]] = self._c_init_queue(csl=csl,
                                                                           init_values=init_values,
                                                                           capacity=capacity,
                                                                           manager=manager,
                                                                           **kwargs)
            assert self.__f_queue.qsize() == num_values

            super().__init__(csl=csl,
                             capacity=capacity)

        @abc.abstractmethod
        def _c_init_queue(self,
                          csl: cs.En.CSL,
                          init_values: con.Pr.SizedIterable[TYPE],
                          capacity: Optional[int] = None,
                          manager: Optional[mp_mngr.SyncManager] = None,
                          **kwargs: Any) -> con.Pr.Queue[TYPE]:
            """
            Note: Returned queue should provide csl safe qsize, put and get methods.
            Then no locking is required here.
            """
            ...

        @final
        def qsize(self, /) -> int:
            return self.__f_queue.qsize()

        @final
        def put(self,
                item: TYPE,
                block: bool = True,
                timeout: Optional[float] = None, /) -> None:
            self.c_put(item=item,
                       blocking=block,
                       timeout=timeout)

        @final
        def get(self,
                block: bool = True,
                timeout: Optional[float] = None, /) -> TYPE:
            return self.c_get(blocking=block,
                              timeout=timeout)

        def c_put(self,
                  item: TYPE,
                  blocking: bool = True,
                  timeout: Optional[float] = None) -> None:

            self.__f_queue.put(item,
                               blocking,
                               (None
                                if timeout is None
                                else max(0.0, timeout)))

        def c_get(self,
                  blocking: bool = True,
                  timeout: Optional[float] = None) -> TYPE:
            return self.__f_queue.get(blocking,
                                      (None
                                       if timeout is None
                                       else max(0.0, timeout)))

    class CSChunkCapacityQueue(CSCapacityQueue[Tuple[TYPE, ...]],
                               Pr.CSChunkCapacityQueue[TYPE],
                               abc.ABC):

        __f_min_chunk_size: Final[Optional[int]]
        __f_max_chunk_size: Final[Optional[int]]
        __f_has_chunk_size_bounds: Final[bool]

        def __init__(self,
                     csl: cs.En.CSL,
                     init_values: con.Pr.SizedIterable[Tuple[TYPE, ...]],
                     capacity: Optional[int] = None,
                     min_chunk_size: Optional[int] = None,
                     max_chunk_size: Optional[int] = None,
                     manager: Optional[mp_mngr.SyncManager] = None,
                     **kwargs: Any):
            if min_chunk_size is not None:
                if min_chunk_size <= 0:
                    raise ValueError(f"'min_chunk_size' must be > 0 (is: {min_chunk_size}).")

            if max_chunk_size is not None:
                if max_chunk_size <= 0:
                    raise ValueError(f"'max_chunk_size' must be > 0 (is: {max_chunk_size}).")

            if min_chunk_size is not None and max_chunk_size is not None:
                if min_chunk_size > max_chunk_size:
                    raise ValueError(f"If both 'min_chunk_size' and 'max_chunk_size' are provided, "
                                     f"'min_chunk_size' must be <= 'max_chunk_size' "
                                     f"(here: {min_chunk_size} vs {max_chunk_size}).")

            self.__f_min_chunk_size = min_chunk_size
            self.__f_max_chunk_size = max_chunk_size
            self.__f_has_chunk_size_bounds = (min_chunk_size is not None
                                              or max_chunk_size is not None)

            super().__init__(csl=csl,
                             init_values=init_values,
                             capacity=capacity,
                             manager=manager,
                             **kwargs)

        @final
        def c_get_min_chunk_size(self) -> Optional[int]:
            return self.__f_min_chunk_size

        @final
        def c_get_max_chunk_size(self) -> Optional[int]:
            return self.__f_max_chunk_size

        @final
        def c_put(self,
                  item: Tuple[TYPE, ...],
                  blocking: bool = True,
                  timeout: Optional[float] = None) -> None:

            if self.__f_has_chunk_size_bounds:
                item_len: Final[int] = len(item)

                if (self.__f_min_chunk_size is not None
                        and item_len < self.__f_min_chunk_size):
                    raise ValueError(f"Provided item length is too small (is {item_len}, "
                                     f"minimum chunk size is {self.__f_min_chunk_size}.")

                if (self.__f_max_chunk_size is not None
                        and item_len > self.__f_max_chunk_size):
                    raise ValueError(f"Provided item length is too large (is {item_len}, "
                                     f"maximum chunk size is {self.__f_max_chunk_size}.")

            super().c_put(item=item,
                          blocking=blocking,
                          timeout=timeout)

        @final
        def c_get(self,
                  blocking: bool = True,
                  timeout: Optional[float] = None) -> Tuple[TYPE, ...]:

            result: Tuple[TYPE, ...] = super().c_get(blocking=blocking,
                                                     timeout=timeout)

            # noinspection PyUnreachableCode
            if __debug__ and self.__f_has_chunk_size_bounds:
                item_len: Final[int] = len(result)

                if self.__f_min_chunk_size is not None:
                    assert item_len >= self.__f_min_chunk_size

                if self.__f_max_chunk_size is not None:
                    assert item_len <= self.__f_max_chunk_size

            return result


class Er:
    class CloseableError(ValueError):
        pass


class Wa:
    class CloseableWarning(RuntimeWarning):
        pass


class Im:

    class CSData(Ab.CSData[TYPE]):

        def __init__(self,
                     csl: cs.En.CSL,
                     initial_value: TYPE,
                     manager: Optional[mp_mngr.SyncManager] = None):

            super().__init__(csl=csl,
                             initial_value=initial_value,
                             manager=manager)

        @final
        def _c_init_value(self,
                          csl: cs.En.CSL,
                          initial_value: TYPE,
                          manager: Optional[mp_mngr.SyncManager] = None,
                          **kwargs: Any) -> rcsd.Pr.Value[TYPE]:
            assert len(kwargs) == 0
            return rcsd.Ca.c_to_cs_value(csl=csl,
                                         init_value=initial_value,
                                         manager=manager)

    class CSMutableSequence(Ab.CSMutableSequence[TYPE]):

        def __init__(self,
                     csl: cs.En.CSL,
                     initial_values: con.Pr.Iterable[TYPE],
                     manager: Optional[mp_mngr.SyncManager] = None):

            super().__init__(csl=csl,
                             initial_values=initial_values,
                             manager=manager)

        @final
        def _c_init_mra(self,
                        csl: cs.En.CSL,
                        initial_values: con.Pr.Iterable[TYPE],
                        manager: Optional[mp_mngr.SyncManager] = None,
                        **kwargs: Any) -> con.Pr.MutableRandomAccess[TYPE]:
            assert len(kwargs) == 0
            return rcsd.Ca.c_to_cs_mra(csl=csl,
                                       init_values=initial_values,
                                       manager=manager)

    class CSMutableLengthSequence(Ab.CSMutableLengthSequence[TYPE]):

        def __init__(self,
                     csl: cs.En.CSL,
                     initial_values: con.Pr.Iterable[TYPE],
                     manager: Optional[mp_mngr.SyncManager] = None):

            super().__init__(csl=csl,
                             initial_values=initial_values,
                             manager=manager)

        @final
        def _c_init_mra(self,
                        csl: cs.En.CSL,
                        initial_values: con.Pr.Iterable[TYPE],
                        manager: Optional[mp_mngr.SyncManager] = None,
                        **kwargs: Any) -> con.Pr.MutableLengthRandomAccess[TYPE]:
            assert len(kwargs) == 0
            return rcsd.Ca.c_to_cs_mlseq(csl=csl,
                                         init_values=initial_values,
                                         manager=manager)

    class CSCapacityQueue(Ab.CSCapacityQueue[TYPE]):

        def __init__(self,
                     csl: cs.En.CSL,
                     init_values: con.Pr.SizedIterable[TYPE],
                     capacity: Optional[int] = None,
                     manager: Optional[mp_mngr.SyncManager] = None):

            super().__init__(csl=csl,
                             init_values=init_values,
                             capacity=capacity,
                             manager=manager)

        @final
        def _c_init_queue(self,
                          csl: cs.En.CSL,
                          init_values: con.Pr.SizedIterable[TYPE],
                          capacity: Optional[int] = None,
                          manager: Optional[mp_mngr.SyncManager] = None,
                          **kwargs: Any) -> con.Pr.Queue[TYPE]:

            assert len(kwargs) == 0
            return rcsd.Ca.c_to_cs_queue(csl=csl,
                                         init_values=init_values,
                                         capacity=capacity,
                                         manager=manager)

    class CSChunkCapacityQueue(Ab.CSChunkCapacityQueue[TYPE]):

        def __init__(self,
                     csl: cs.En.CSL,
                     init_values: con.Pr.SizedIterable[Tuple[TYPE, ...]],
                     capacity: Optional[int] = None,
                     min_chunk_size: Optional[int] = None,
                     max_chunk_size: Optional[int] = None,
                     manager: Optional[mp_mngr.SyncManager] = None):

            super().__init__(csl=csl,
                             init_values=init_values,
                             capacity=capacity,
                             min_chunk_size=min_chunk_size,
                             max_chunk_size=max_chunk_size,
                             manager=manager)

        @final
        def _c_init_queue(self,
                          csl: cs.En.CSL,
                          init_values: con.Pr.SizedIterable[Tuple[TYPE, ...]],
                          capacity: Optional[int] = None,
                          manager: Optional[mp_mngr.SyncManager] = None,
                          **kwargs: Any) -> con.Pr.Queue[Tuple[TYPE, ...]]:

            assert len(kwargs) == 0
            return rcsd.Ca.c_to_cs_queue(csl=csl,
                                         init_values=init_values,
                                         capacity=capacity,
                                         manager=manager)

    class CSCloseableMixin(csrwlocks.Im.CSRWLockableMixin,
                           Pr.CSCloseableMixin):

        __f_is_closed: Final[Pr.CSData[bool]]

        def __init__(self,
                     *args: Any,
                     csl: cs.En.CSL,
                     csrwlock_keys: Optional[con.Pr.Iterable[str]] = None,
                     manager: Optional[mp_mngr.SyncManager] = None,
                     **kwargs: Any):

            super().__init__(*args,
                             csl=csl,
                             csrwlock_keys=csrwlock_keys,
                             manager=manager,
                             **kwargs)

            self.__f_is_closed = Im.CSData(csl=csl,
                                           initial_value=False,
                                           manager=manager)

        @final
        def _c_check_is_closed(self,
                               raise_: bool = True) -> bool:
            """
            Only call with at least r-locked context.
            """
            is_closed: Final[bool] = self.__f_is_closed.c_get(_unsafe=True)

            if not is_closed and raise_:
                raise Er.CloseableError(f"Method cannot be called for unclosed {Pr.CSCloseableMixin.__name__}")

            return is_closed

        @final
        def _c_check_is_unclosed(self,
                                 raise_: bool = True) -> bool:
            """
            Only call with at least r-locked context.
            """
            is_unclosed: Final[bool] = not self.__f_is_closed.c_get(_unsafe=True)

            if not is_unclosed and raise_:
                raise Er.CloseableError(f"Method cannot be called for closed {Pr.CSCloseableMixin.__name__}")

            return is_unclosed

        @csrwlocks.De.d_cswlock
        def c_close(self,
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> bool:
            is_unclosed: Final[bool] = self._c_check_is_unclosed(raise_=False)

            if is_unclosed:
                self._c_close()

            assert self._c_check_is_closed()
            return is_unclosed

        @abc.abstractmethod
        def _c_close(self) -> None:
            """
            Guaranteed to be called only once while holding w-lock in unclosed state.
            """
            ...

        @csrwlocks.De.d_csrlock
        def c_is_closed(self,
                        blocking: bool = True,
                        timeout: Optional[float] = None,
                        _unsafe: bool = False) -> bool:
            return self.__f_is_closed.c_get(_unsafe=True)
