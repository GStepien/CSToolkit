from __future__ import annotations

import abc
from functools import wraps
from typing import Protocol, runtime_checkable, Optional, Final, final, Any, overload, Callable, Union, \
    Tuple, Dict, cast
import multiprocessing.managers as mp_mngr

from concurrency import cslocks, cs, _raw_csdata
from utils.datastructures.frozenmapping import FrozenMapping
from utils.dynamic.inspection import ParamValue, c_get_self_getter, c_get_callable_param_getter, CallableSignatureError
from utils.types.casts import c_assert_not_none, c_assert_type
from utils.types.typevars import CALLABLE_TYPE, TYPE
import utils.types.containers as con


class Co:
    f_DEFAULT_CSRWLOCK_KEY: Final[str] = "__default_csrwlock_key__"


class Pr:

    @runtime_checkable
    class CSRLock(cslocks.Pr.CSLock,
                  Protocol):

        @abc.abstractmethod
        def c_get_reader_num(self) -> int:
            """
            Result is immediately outdated.
            """
            ...

        @abc.abstractmethod
        def c_is_held_reading(self) -> bool:
            """
            Result is immediately outdated.
            """
            ...

    @runtime_checkable
    class CSWLock(cslocks.Pr.CSLock,
                  Protocol):

        @abc.abstractmethod
        def c_is_held_writing(self) -> bool:
            """
            Result is immediately outdated.
            """
            ...

    @runtime_checkable
    class CSRWLock(Protocol):
        """
        Note: At this moment, ability to downgrade is not guaranteed by this interface.
        Also no guarantee about possible reader/writer starvation.
        """
        @abc.abstractmethod
        def c_get_csrlock(self) -> Pr.CSRLock:
            """
            Result may safely be cached.
            """
            ...

        @abc.abstractmethod
        def c_get_cswlock(self) -> Pr.CSWLock:
            """
            Result may safely be cached.
            """
            ...

        @abc.abstractmethod
        def c_get_reader_num(self) -> int:
            """
            Result is immediately outdated.
            """
            ...

        @abc.abstractmethod
        def c_is_held(self) -> bool:
            """
            Return True if either read or write lock is acquired. Result immediately outdated.
            Result is immediately outdated.
            """
            ...

        @abc.abstractmethod
        def c_is_held_writing(self) -> bool:
            """
            Result is immediately outdated.
            """
            ...

        @abc.abstractmethod
        def c_is_held_reading(self) -> bool:
            """
            Result is immediately outdated.
            """
            ...

    # noinspection PyProtectedMember
    @runtime_checkable
    class CSRWLockableMixin(cs.Pr.HasCSLMixin,
                            Protocol):
        """
        A resource whose mutable fields are synchronized by CSRWLocks.
        """

        @abc.abstractmethod
        def c_get_csrwlock_map(self) -> con.Pr.Mapping[str, Pr.CSRWLock]:
            ...

        @abc.abstractmethod
        def c_get_csrwlock(self,
                           lock_key: str = ...) -> Pr.CSRWLock:
            ...


class Ab:
    class CSRWLock(cs.Im.HasCSLMixin,
                   Pr.CSRWLock):

        __f_csrlock: Final[Pr.CSRLock]
        __f_cswlock: Final[Pr.CSWLock]

        def __init__(self,
                     csl: cs.En.CSL,
                     **kwargs: Any):
            self.__f_csrlock = self._c_init_csrlock(csl=csl,
                                                    **kwargs)
            self.__f_cswlock = self._c_init_cswlock(csl=csl,
                                                    **kwargs)

            super().__init__(csl=csl)

        @final
        def c_get_csrlock(self) -> Pr.CSRLock:
            return self.__f_csrlock

        @final
        def c_get_cswlock(self) -> Pr.CSWLock:
            return self.__f_cswlock

        @final
        def c_get_reader_num(self) -> int:
            return self.c_get_csrlock().c_get_reader_num()

        @final
        def c_is_held(self) -> bool:
            # At any fixed time, c_get_csrlock().c_is_held() and c_get_cswlock().c_is_held() should
            # return the same result (although this cannot be asserted here without access to internal locks
            # as each call is immediately outdated.
            return self.c_get_csrlock().c_is_held()

        @final
        def c_is_held_writing(self) -> bool:
            return self.c_get_cswlock().c_is_held_writing()

        @final
        def c_is_held_reading(self) -> bool:
            return self.c_get_csrlock().c_is_held_reading()

        @abc.abstractmethod
        def _c_init_csrlock(self,
                            csl: cs.En.CSL,
                            **kwargs: Any) -> Pr.CSRLock:
            """
            Called before _c_init_cswlock in constructor, do not delete any items from kwargs
            """
            ...

        @abc.abstractmethod
        def _c_init_cswlock(self,
                            csl: cs.En.CSL,
                            **kwargs: Any) -> Pr.CSWLock:
            ...


class Im:
    class _CSRWLockHelpers:

        class CSBool(cslocks.Im.CSLockableMixin):
            __f_value: Final[_raw_csdata.Pr.Value[bool]]

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager] = None,
                         init_value: bool = False):
                self.__f_value = _raw_csdata.Ca.c_to_cs_value(csl=csl,
                                                              init_value=init_value,
                                                              manager=manager)
                super().__init__(csl=csl,
                                 manager=manager)

            # noinspection PyUnusedLocal
            @final
            @cslocks.De.d_cslock
            def c_get(self,
                      blocking: bool = True,
                      timeout: Optional[float] = None,
                      _unsafe: bool = False) -> bool:
                return self.__f_value.value

            # noinspection PyUnusedLocal
            @final
            @cslocks.De.d_cslock
            def c_set(self,
                      new_val: bool,
                      blocking: bool = True,
                      timeout: Optional[float] = None,
                      _unsafe: bool = False) -> None:
                self.__f_value.value = new_val

        class CSInt(cslocks.Im.CSLockableMixin):
            __f_value: Final[_raw_csdata.Pr.Value[int]]

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager] = None,
                         init_value: int = 0):
                self.__f_value = _raw_csdata.Ca.c_to_cs_value(csl=csl,
                                                              init_value=init_value,
                                                              manager=manager)
                super().__init__(csl=csl,
                                 manager=manager)

            # noinspection PyUnusedLocal
            @final
            @cslocks.De.d_cslock
            def c_apply(self,
                        func: Callable[[int], Tuple[int, TYPE]],
                        blocking: bool = True,
                        timeout: Optional[float] = None,
                        _unsafe: bool = False) -> TYPE:
                """
                func is called on internal value. First entry of returned tuple is assigned to internal value
                if it differs.
                Second tuple entry is returned.
                """
                old_val: Final[int] = self.__f_value.value
                result: Tuple[int, TYPE] = func(old_val)
                if result[0] != old_val:
                    self.__f_value.value = result[0]

                return result[1]

            # noinspection PyUnusedLocal
            @final
            @cslocks.De.d_cslock
            def c_get(self,
                      blocking: bool = True,
                      timeout: Optional[float] = None,
                      _unsafe: bool = False) -> int:
                return self.__f_value.value

            # noinspection PyUnusedLocal
            @final
            @cslocks.De.d_cslock
            def c_set(self,
                      new_val: int,
                      blocking: bool = True,
                      timeout: Optional[float] = None,
                      _unsafe: bool = False) -> None:
                self.__f_value.value = new_val

            @final
            def c_add(self,
                      num: int,
                      blocking: bool = True,
                      timeout: Optional[float] = None,
                      _unsafe: bool = False) -> int:
                """
                Returns new value.
                """
                return self.c_apply(func=lambda old_val: (res := old_val + num, res),
                                    blocking=blocking,
                                    timeout=timeout,
                                    _unsafe=_unsafe)

        class RLock(cslocks.Ab.Lock):
            __f_reader_num: Final[Im._CSRWLockHelpers.CSInt]
            __f_internal_csrlock: Final[cslocks.Pr.Lock]
            __f_internal_cswlock: Final[cslocks.Pr.Lock]  # Shared with WLock
            """
            NOTE: This r-lock's c_release will block if w-lock is currently acquired by a writer and
            a reader is already waiting on w-lock in c_acquire!
            Nonetheless, this should only happen in bad code design where c_release is called without actually having 
            acquired the r-lock previously.
            """
            def __init__(self,
                         reader_num: Im._CSRWLockHelpers.CSInt,
                         internal_csrlock: cslocks.Pr.Lock,
                         internal_cswlock: cslocks.Pr.Lock):
                assert reader_num.c_get() == 0
                self.__f_reader_num = reader_num
                self.__f_internal_csrlock = internal_csrlock
                self.__f_internal_cswlock = internal_cswlock
                super().__init__()

            @final
            def c_is_reentrant(self) -> bool:
                return True

            @final
            def c_get_reader_num(self) -> int:
                return self.__f_reader_num.c_get()

            @final
            def c_is_held(self) -> bool:
                # Return True if either read or write lock is acquired. Result immediately outdated.
                return self.__f_internal_cswlock.c_is_held()

            # noinspection PyProtectedMember
            @final
            def c_acquire(self,
                          blocking: bool = True,
                          timeout: Optional[float] = None) -> bool:

                start_perf_counter: Optional[float] = cslocks.Ca.c_timeout_start_perf_counter(blocking=blocking,
                                                                                              timeout=timeout)
                # Might block if a different reader is already waiting for w-lock
                with self.__f_internal_csrlock.c_acquire_timeout(blocking=blocking,
                                                                 timeout=timeout) as success:
                    if not success:
                        return False
                    else:
                        result: bool

                        old_val: Final[int] = self.__f_reader_num.c_get()
                        assert old_val >= 0

                        if old_val == 0:
                            # new_val > 1 implies old_val > 0 which implies we are already holding w-lock
                            if not self.__f_internal_cswlock.c_acquire(blocking=blocking,
                                                                       timeout=cslocks.Ca.c_update_timeout(
                                                                           start_perf_counter=start_perf_counter,
                                                                           blocking=blocking,
                                                                           timeout=timeout)):
                                result = False
                            else:
                                result = True
                        else:
                            # W-Lock should be held at this point
                            assert self.__f_internal_cswlock.c_is_held()
                            result = True

                        if result:
                            self.__f_reader_num.c_add(num=1)
                            assert self.__f_reader_num.c_get() == old_val + 1 > 0

                        return result

            @final
            def c_release(self) -> None:
                # This might block if some writer has the lock and another reader
                # is already waiting on w-lock in c_acquire
                with self.__f_internal_csrlock as success_:
                    assert success_

                    if self.__f_reader_num.c_get() == 0:
                        raise ValueError(f"Called c_release on non-held read lock.")

                    assert self.__f_reader_num.c_get() > 0 and self.__f_internal_cswlock.c_is_held()

                    # noinspection PyUnreachableCode
                    if __debug__:
                        # noinspection PyUnusedLocal
                        old_val: Final[int] = self.__f_reader_num.c_get()

                    new_val: Final[int] = self.__f_reader_num.c_add(num=-1)
                    # noinspection PyUnreachableCode
                    if __debug__:
                        assert new_val == old_val - 1 == self.__f_reader_num.c_get() >= 0

                    if new_val == 0:
                        self.__f_internal_cswlock.c_release()

        @staticmethod
        def c_retrieve_helpers(delete_after: bool,
                               kwargs: con.Pr.MutableMapping[str, Any]) -> Tuple[Optional[CSBool],
                                                                                 Optional[CSInt],
                                                                                 Optional[cslocks.Pr.Lock],
                                                                                 Optional[cslocks.Pr.Lock]]:
            """
            Returns held_writing or None, reader_num or None, internal_csrlock or None, internal_cswlock or None
            """

            assert 'held_writing' not in kwargs or isinstance(kwargs['held_writing'], Im._CSRWLockHelpers.CSBool)
            assert 'reader_num' not in kwargs or isinstance(kwargs['reader_num'], Im._CSRWLockHelpers.CSInt)
            assert 'internal_csrlock' not in kwargs or isinstance(kwargs['internal_csrlock'], cslocks.Pr.Lock)
            assert 'internal_cswlock' not in kwargs or isinstance(kwargs['internal_cswlock'], cslocks.Pr.Lock)

            held_writing: Final[Optional[Im._CSRWLockHelpers.CSBool]] = (None
                                                                         if 'held_writing' not in kwargs
                                                                         else kwargs['held_writing'])
            reader_num: Final[Optional[Im._CSRWLockHelpers.CSInt]] = (None
                                                                      if 'reader_num' not in kwargs
                                                                      else kwargs['reader_num'])
            internal_csrlock: Final[Optional[cslocks.Pr.Lock]] = (None
                                                                  if 'internal_csrlock' not in kwargs
                                                                  else kwargs['internal_csrlock'])
            internal_cswlock: Final[Optional[cslocks.Pr.Lock]] = (None
                                                                  if 'internal_cswlock' not in kwargs
                                                                  else kwargs['internal_cswlock'])

            if delete_after:
                if 'held_writing' in kwargs:
                    del kwargs['held_writing']
                if 'reader_num' in kwargs:
                    del kwargs['reader_num']
                if 'internal_csrlock' in kwargs:
                    del kwargs['internal_csrlock']
                if 'internal_cswlock' in kwargs:
                    del kwargs['internal_cswlock']

            return held_writing, reader_num, internal_csrlock, internal_cswlock

        class CSRLock(cslocks.Ab.CSLock,
                      Pr.CSRLock):
            """
            NOTE: Underlying r-lock is an `Im._CSRWLockHelpers.RLock` instance.
            The former's c_release will block if w-lock is currently acquired by a writer and
            a reader is already waiting on w-lock in c_acquire!
            Nonetheless, this should only happen in bad code design where c_release is called without actually having
            acquired the r-lock previously.
            """
            def __init__(self,
                         csl: cs.En.CSL,
                         reader_num: Im._CSRWLockHelpers.CSInt,
                         internal_csrlock: cslocks.Pr.Lock,
                         internal_cswlock: cslocks.Pr.Lock):

                super().__init__(csl=csl,
                                 reader_num=reader_num,
                                 internal_csrlock=internal_csrlock,
                                 internal_cswlock=internal_cswlock)

            @final
            def c_is_reentrant(self) -> bool:
                return True

            @final
            def c_get_reader_num(self) -> int:
                return c_assert_type(self._c_get_lock(),
                                     Im._CSRWLockHelpers.RLock).c_get_reader_num()

            @final
            def c_is_held_reading(self) -> bool:
                return self.c_get_reader_num() > 0

            @final
            def _c_init_lock(self,
                             csl: cs.En.CSL,
                             **kwargs: Any) -> cslocks.Pr.Lock:

                reader_num: Optional[Im._CSRWLockHelpers.CSInt]
                internal_csrlock: Optional[cslocks.Pr.Lock]
                internal_cswlock: Optional[cslocks.Pr.Lock]

                _, reader_num, internal_csrlock, internal_cswlock = \
                    Im._CSRWLockHelpers.c_retrieve_helpers(delete_after=True,
                                                           kwargs=kwargs)

                assert len(kwargs) == 0

                return Im._CSRWLockHelpers.RLock(reader_num=c_assert_not_none(reader_num),
                                                 internal_csrlock=c_assert_not_none(internal_csrlock),
                                                 internal_cswlock=c_assert_not_none(internal_cswlock))

        class WLock(cslocks.Ab.Lock):
            __f_held_writing: Final[Im._CSRWLockHelpers.CSBool]
            __f_internal_cswlock: Final[cslocks.Pr.Lock]  # Shared with RLock

            def __init__(self,
                         held_writing: Im._CSRWLockHelpers.CSBool,
                         internal_cswlock: cslocks.Pr.Lock):
                assert held_writing.c_get() is False
                self.__f_held_writing = held_writing
                self.__f_internal_cswlock = internal_cswlock
                super().__init__()

            @final
            def c_is_reentrant(self) -> bool:
                return False

            @final
            def c_is_held_writing(self) -> bool:
                return self.__f_held_writing.c_get(blocking=True,
                                                   timeout=None)

            @final
            def c_is_held(self) -> bool:
                # Return True if either read or write lock is acquired. Result immediately outdated.
                return self.__f_internal_cswlock.c_is_held()

            @final
            def c_acquire(self,
                          blocking: bool = True,
                          timeout: Optional[float] = None) -> bool:
                success: bool = self.__f_internal_cswlock.c_acquire(blocking=blocking,
                                                                    timeout=timeout)
                if success:
                    # Only one thread/process can be successful here
                    assert not self.__f_held_writing.c_get(blocking=True,
                                                           timeout=None)
                    self.__f_held_writing.c_set(new_val=True,
                                                blocking=True,
                                                timeout=None)
                    # Only now c_release() will be successful
                return success

            @final
            def c_release(self) -> None:
                # Access to __f_held_writing always in a non-blocking context -> c_release should never block
                with self.__f_held_writing.c_get_cslock() as success:
                    assert success
                    if self.__f_held_writing.c_get(_unsafe=True):
                        self.__f_internal_cswlock.c_release()
                        self.__f_held_writing.c_set(new_val=False,
                                                    _unsafe=True)
                    else:
                        raise ValueError(f"Called c_release on non-held write lock.")

        class CSWLock(cslocks.Ab.CSLock,
                      Pr.CSWLock):
            """
            Note: c_is_held() will will return False if and only if neither the write nor a reading lock
            has been acquired. If it returns True, use the CSRLock's c_get_reader_num method to determine whether
            the csrwlock has been acquired in a writing or reading manner (the former case is true, if the reader num
            is zero and c_is_held() returns True).
            """

            def __init__(self,
                         csl: cs.En.CSL,
                         held_writing: Im._CSRWLockHelpers.CSBool,
                         reader_num: Im._CSRWLockHelpers.CSInt,
                         internal_csrlock: cslocks.Pr.Lock,
                         internal_cswlock: cslocks.Pr.Lock):

                super().__init__(csl=csl,
                                 held_writing=held_writing,
                                 reader_num=reader_num,
                                 internal_csrlock=internal_csrlock,
                                 internal_cswlock=internal_cswlock)

            @final
            def c_is_reentrant(self) -> bool:
                return False

            @final
            def c_is_held_writing(self) -> bool:
                return c_assert_type(self._c_get_lock(),
                                     Im._CSRWLockHelpers.WLock).c_is_held_writing()

            @final
            def _c_init_lock(self,
                             csl: cs.En.CSL,
                             **kwargs: Any) -> cslocks.Pr.Lock:
                held_writing: Optional[Im._CSRWLockHelpers.CSBool]
                internal_cswlock: Optional[cslocks.Pr.Lock]

                held_writing, _, __, internal_cswlock = \
                    Im._CSRWLockHelpers.c_retrieve_helpers(delete_after=True,
                                                           kwargs=kwargs)

                assert len(kwargs) == 0

                return Im._CSRWLockHelpers.WLock(held_writing=c_assert_not_none(held_writing),
                                                 internal_cswlock=c_assert_not_none(internal_cswlock))

    class CSRWLock(Ab.CSRWLock):

        def __init__(self,
                     csl: cs.En.CSL,
                     manager: Optional[mp_mngr.SyncManager] = None):

            # noinspection PyProtectedMember
            super().__init__(csl=csl,
                             held_writing=Im._CSRWLockHelpers.CSBool(csl=csl,
                                                                     manager=manager,
                                                                     init_value=False),
                             reader_num=Im._CSRWLockHelpers.CSInt(csl=csl,
                                                                  manager=manager,
                                                                  init_value=0),
                             internal_csrlock=cslocks.Ca.c_lock_from_csl(csl=csl,
                                                                         manager=manager),
                             internal_cswlock=cslocks.Ca.c_lock_from_csl(csl=csl,
                                                                         manager=manager))

        @final
        def _c_init_csrlock(self,
                            csl: cs.En.CSL,
                            **kwargs: Any) -> Pr.CSRLock:
            reader_num: Optional[Im._CSRWLockHelpers.CSInt]
            internal_csrlock: Optional[cslocks.Pr.Lock]
            internal_cswlock: Optional[cslocks.Pr.Lock]

            _, reader_num, internal_csrlock, internal_cswlock = \
                Im._CSRWLockHelpers.c_retrieve_helpers(delete_after=False,
                                                       kwargs=kwargs)
            assert len(kwargs) == 4

            return Im._CSRWLockHelpers.CSRLock(csl=csl,
                                               reader_num=c_assert_not_none(reader_num),
                                               internal_csrlock=c_assert_not_none(internal_csrlock),
                                               internal_cswlock=c_assert_not_none(internal_cswlock))

        def _c_init_cswlock(self,
                            csl: cs.En.CSL,
                            **kwargs: Any) -> Pr.CSWLock:
            held_writing: Optional[Im._CSRWLockHelpers.CSBool]
            reader_num: Optional[Im._CSRWLockHelpers.CSInt]
            internal_csrlock: Optional[cslocks.Pr.Lock]
            internal_cswlock: Optional[cslocks.Pr.Lock]

            held_writing, reader_num, internal_csrlock, internal_cswlock = \
                Im._CSRWLockHelpers.c_retrieve_helpers(delete_after=False,
                                                       kwargs=kwargs)
            assert len(kwargs) == 4

            return Im._CSRWLockHelpers.CSWLock(csl=csl,
                                               held_writing=c_assert_not_none(held_writing),
                                               reader_num=c_assert_not_none(reader_num),
                                               internal_csrlock=c_assert_not_none(internal_csrlock),
                                               internal_cswlock=c_assert_not_none(internal_cswlock))

    class CSRWLockableMixin(cs.Im.HasCSLMixin,
                            Pr.CSRWLockableMixin):

        __f_csrwlock_map: Final[con.Pr.Mapping[str, Pr.CSRWLock]]
        __f_frozen_csrwlock_map_view: Final[con.Pr.Mapping[str, Pr.CSRWLock]]

        def __init__(self,
                     *args: Any,
                     csl: cs.En.CSL,
                     csrwlock_keys: Optional[con.Pr.Iterable[str]] = None,
                     manager: Optional[mp_mngr.SyncManager] = None,
                     **kwargs: Any):
            super().__init__(*args,
                             csl=csl,
                             **kwargs)

            self.__f_csrwlock_map = dict()

            if csrwlock_keys is None:
                csrwlock_keys = []
            else:
                csrwlock_keys = list(csrwlock_keys)

            csrwlock_key_set: con.Pr.MutableSet[str] = set(csrwlock_keys)
            if len(csrwlock_key_set) != len(csrwlock_keys):
                raise ValueError(f"CSLock names must be unique.")

            if Co.f_DEFAULT_CSRWLOCK_KEY not in csrwlock_key_set:
                csrwlock_key_set.add(Co.f_DEFAULT_CSRWLOCK_KEY)

            # noinspection PyTypeChecker
            for csrwlock_name in csrwlock_key_set:
                self.__f_csrwlock_map[csrwlock_name] = Im.CSRWLock(csl=csl,
                                                                   manager=manager)
            self.__f_frozen_csrwlock_map_view = FrozenMapping(self.__f_csrwlock_map,
                                                              as_view=True)

        @final
        def c_get_csrwlock_map(self) -> con.Pr.Mapping[str, Pr.CSRWLock]:
            return self.__f_frozen_csrwlock_map_view

        @final
        def c_get_csrwlock(self,
                           lock_key: str = Co.f_DEFAULT_CSRWLOCK_KEY) -> Pr.CSRWLock:
            try:
                return self.__f_csrwlock_map[lock_key]
            except KeyError as e:
                raise cslocks.Er.UnknownLockName from e


class De:

    @staticmethod
    @overload
    def d_csrlock(method_or_lock_name: CALLABLE_TYPE) -> CALLABLE_TYPE:
        ...

    @staticmethod
    @overload
    def d_csrlock(method_or_lock_name: str) -> Callable[[CALLABLE_TYPE], CALLABLE_TYPE]:
        ...

    @staticmethod
    def d_csrlock(method_or_lock_name: Union[CALLABLE_TYPE, str]) -> Union[CALLABLE_TYPE,
                                                                           Callable[[CALLABLE_TYPE], CALLABLE_TYPE]]:
        return De._d_csrwlock(method_or_lock_name=method_or_lock_name,
                              reader_lock=True)

    @staticmethod
    @overload
    def d_cswlock(method_or_lock_name: CALLABLE_TYPE) -> CALLABLE_TYPE:
        ...

    @staticmethod
    @overload
    def d_cswlock(method_or_lock_name: str) -> Callable[[CALLABLE_TYPE], CALLABLE_TYPE]:
        ...

    @staticmethod
    def d_cswlock(method_or_lock_name: Union[CALLABLE_TYPE, str]) -> Union[CALLABLE_TYPE,
                                                                           Callable[[CALLABLE_TYPE], CALLABLE_TYPE]]:
        return De._d_csrwlock(method_or_lock_name=method_or_lock_name,
                              reader_lock=False)

    @staticmethod
    def _d_csrwlock(method_or_lock_name: Union[CALLABLE_TYPE, str],
                    reader_lock: bool) -> Union[CALLABLE_TYPE,
                                                Callable[[CALLABLE_TYPE], CALLABLE_TYPE]]:

        def c_csrwlock_getter(csrwlockable: Pr.CSRWLockableMixin) -> Pr.CSRWLock:
            if isinstance(method_or_lock_name, str):
                return csrwlockable.c_get_csrwlock(lock_key=method_or_lock_name)
            else:
                return csrwlockable.c_get_csrwlock()

        def d_csrwlock_(callable_: CALLABLE_TYPE) -> CALLABLE_TYPE:

            param_getter: Callable[[str, Tuple[Any, ...], Dict[str, Any]], ParamValue] = \
                c_get_callable_param_getter(callable_=callable_)
            self_getter: Callable[[Tuple[Any, ...], Dict[str, Any]], ParamValue] = \
                c_get_self_getter(callable_=callable_,
                                  _call_param_getter=param_getter)

            def c_get_self(args: Tuple[Any, ...],
                           kwargs: Dict[str, Any]) -> Pr.CSRWLockableMixin:
                self: Any = self_getter(args, kwargs).value
                assert isinstance(self, Pr.CSRWLockableMixin)
                return self

            try:
                # If default provided, must be False
                val: Any = param_getter('_unsafe', (), {}).value
                if val is not False:
                    raise CallableSignatureError(f"If a 'd_csrlock' or 'd_cswlock' decorated function has a default "
                                                 f"value for '_unsafe', it must be False.")
            except ValueError:
                # At this point either no default provided or no '_unsafe' parameter exists
                try:
                    param_getter('_unsafe', (), {'_unsafe': False}).value
                except ValueError:
                    raise CallableSignatureError(f"A 'd_csrlock' or 'd_cswlock' decorated function must take "
                                                 f"parameters 'blocking' (bool), 'timeout' (Optional[float]) "
                                                 f"and _unsafe (bool).")

            try:
                param_getter('blocking', (), {'blocking': True}).value
            except ValueError:
                raise CallableSignatureError(f"A 'd_csrlock' or 'd_cswlock' decorated function must take "
                                             f"parameters 'blocking' (bool), 'timeout' (Optional[float]) "
                                             f"and _unsafe (bool).")

            try:
                param_getter('timeout', (), {'timeout': None}).value
            except ValueError:
                raise CallableSignatureError(f"A 'd_csrlock' or 'd_cswlock' decorated function must take "
                                             f"parameters 'blocking' (bool), 'timeout' (Optional[float]) "
                                             f"and _unsafe (bool).")

            @wraps(callable_)
            def d_csrwlock__(*args: Any, **kwargs: Any) -> Any:

                unsafe_: Any = param_getter('_unsafe', args, kwargs).value
                assert isinstance(unsafe_, bool)
                unsafe: Final[bool] = unsafe_

                blocking_: Any = param_getter('blocking', args, kwargs).value
                assert isinstance(blocking_, bool)
                blocking: Final[bool] = blocking_

                timeout_: Any = param_getter('timeout', args, kwargs).value
                if isinstance(timeout_, int):
                    timeout_ = float(timeout_)
                assert timeout_ is None or isinstance(timeout_, float)
                timeout: Final[Optional[float]] = timeout_

                if not unsafe:

                    self: Final[Pr.CSRWLockableMixin] = c_get_self(args=args, kwargs=kwargs)
                    csrwlock: Final[Pr.CSRWLock] = c_csrwlock_getter(csrwlockable=self)

                    cslock: Final[cslocks.Pr.CSLock] = (csrwlock.c_get_csrlock()
                                                        if reader_lock
                                                        else csrwlock.c_get_cswlock())

                    with cslock.c_acquire_timeout(blocking=blocking, timeout=timeout) as success:
                        if not success:
                            raise TimeoutError()
                        else:
                            return callable_(*args, **kwargs)
                else:
                    # _unsafe = True
                    return callable_(*args, **kwargs)

            return cast(CALLABLE_TYPE, d_csrwlock__)

        return (d_csrwlock_
                if isinstance(method_or_lock_name, str)
                else d_csrwlock_(method_or_lock_name))
