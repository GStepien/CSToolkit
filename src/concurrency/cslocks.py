from __future__ import annotations

import abc
import threading
import time
from contextlib import contextmanager
from functools import wraps
from types import TracebackType
from typing import Protocol, runtime_checkable, Optional, Type, Final, final, Literal, Any, overload, Callable, Union, \
    Tuple, Dict, cast
import multiprocessing.managers as mp_mngr

import utils.types.magicmethods as gmm
import utils.types.containers as con
from concurrency import cs
from utils.datastructures.frozenmapping import FrozenMapping
from utils.dynamic.inspection import ParamValue, c_get_self_getter, c_get_callable_param_getter, CallableSignatureError
from utils.types.typevars import CALLABLE_TYPE


class Co:
    f_POLLING_TIMEOUT: Final[float] = 0.1
    f_DEFAULT_CSLOCK_KEY: Final[str] = "__default_cslock_key__"


class Ca:

    @staticmethod
    def c_lock_from_csl(csl: cs.En.CSL,
                        manager: Optional[mp_mngr.SyncManager]) -> Pr.Lock:
        result: Pr.Lock
        if csl == cs.En.CSL.SINGLE_THREAD:
            result = _Im.LockSingleThread()
        elif csl == cs.En.CSL.MULTI_THREAD:
            result = _Im.LockMultithreadAdapter(tlock=threading.Lock())
        elif csl == cs.En.CSL.MULTI_PROCESS:
            if manager is None:
                raise ValueError(f"Manager required for CSL '{cs.En.CSL.MULTI_PROCESS.name}'")
            result = _Im.LockMultithreadAdapter(tlock=manager.Lock())
        else:
            raise ValueError(f"Unsupported CS level: {csl.name}")

        return result

    @staticmethod
    def c_timeout_start_perf_counter(blocking: bool,
                                     timeout: Optional[float]) -> Optional[float]:
        if blocking and timeout is not None:
            return time.perf_counter()
        else:
            return None

    @staticmethod
    def c_update_timeout(start_perf_counter: Optional[float],
                         blocking: bool,
                         timeout: Optional[float]) -> Optional[float]:
        update: Final[bool] = blocking and timeout is not None
        assert (start_perf_counter is not None) == update

        if update:
            assert start_perf_counter is not None and timeout is not None
            delta: Final[float] = time.perf_counter() - start_perf_counter
            assert delta >= 0.0
            return max(0.0, timeout - delta)
        else:
            return None


class Pr:
    @runtime_checkable
    class _LockThreading(gmm.Pr.ContextManager[bool],
                         Protocol):
        """
        Minimal protocol for threading.Lock (which is, more or less, also the type returned
        by SyncManager.Lock(), see also comment at Pr.Lock).
        """

        @abc.abstractmethod
        def acquire(self,
                    blocking: bool = ...,
                    timeout: float = ...) -> bool:
            ...

        @abc.abstractmethod
        def release(self) -> None:
            """
            threading.Lock raises RuntimeError when already unlocked.
            """
            ...

    @runtime_checkable
    class _LockMultiprocessing(gmm.Pr.ContextManager[bool],
                               Protocol):
        """
        Minimal protocol for multiprocessing.Lock, see also comment at Pr.Lock.
        """

        @abc.abstractmethod
        def acquire(self,
                    block: bool = ...,
                    timeout: Optional[float] = ...) -> bool:
            ...

        @abc.abstractmethod
        def release(self) -> None:
            """
            multiprocessing.Lock raises ValueError when already unlocked.
            """
            ...

    @runtime_checkable
    class Lock(gmm.Pr.ContextManager[bool],
               Protocol):
        """
        Unified lock protocol

        Unfortunately, the acquire() methods of multiprocessing.Lock, threading.Lock and
        SyncManager.Lock are not quite consistent:
        * threading.Lock.acquire:
            * Does NOT accept timeout=None.
            * Does NOT accept negative timeout with the exception of -1 which represents
              and infinite timeout.
            * Does ONLY allow timeout=-1 if blocking=False.

        * multiprocessing.Lock.acquire:
            * The blocking parameter is called 'block' instead of 'blocking'.
            * Accepts negative timeout which is treated as a timeout of 0 (including -1).
            * timeout=None represents an infinite timeout.
            * Allows arbitrary timeout values if block=False.

        * SyncManager.Lock.acquire:
            * Almost like threading.Lock but also accepts timeout=None as infinite timeout.
            * Consequently, does ONLY allow timeout=-1 OR timeout=None if blocking=False.

        * This protocol behaves as follows:
            * If blocking=False, the timeout parameter is ignored.
            * If blocking=True: timeout=None represents an infinite timeout. From a non-None timeout,
              the actual timeout is computed via max(0.0, timeout)

        Use Im.LockMultithreadAdapter to make a threading.Lock compatible with this protocol.
        Use Im.LockMultiprocessAdapter to make a multiprocessing.Lock compatible with this protocol.
        """

        @abc.abstractmethod
        def c_is_reentrant(self) -> bool:
            ...

        @abc.abstractmethod
        def c_acquire(self,
                      blocking: bool = ...,
                      timeout: Optional[float] = ...) -> bool:
            ...

        @abc.abstractmethod
        def c_release(self) -> None:
            """
            Should raise a ValueError if already held (note: threading Lock throws RuntimeError and
            multiprocessing Lock throws ValueError in that case).
            """
            ...

        @abc.abstractmethod
        def c_is_held(self) -> bool:
            """
            Result is immediately outdated.
            """
            ...

        @abc.abstractmethod
        def c_acquire_timeout(self,
                              blocking: bool = ...,
                              timeout: Optional[float] = ...) -> gmm.Pr.ContextManager[bool]:
            ...

    @runtime_checkable
    class CSLock(Lock,
                 cs.Pr.HasCSLMixin,
                 Protocol):
        """
        Contract: Implementation MUST be non-reentrant.

        CSL lock interface for explicitly non-reentrant (also called non-recursive) locks.
        """
        ...

    @runtime_checkable
    class CSLockableMixin(cs.Pr.HasCSLMixin,
                          Protocol):
        """
        A resource whose mutable fields are synchronized by CSLocks.
        """

        @abc.abstractmethod
        def c_get_cslock_map(self) -> con.Pr.Mapping[str, Pr.CSLock]:
            ...

        @abc.abstractmethod
        def c_get_cslock(self,
                         lock_key: str = ...) -> Pr.CSLock:
            ...


class Ab:
    class Lock(Pr.Lock,
               abc.ABC):

        def c_is_held(self) -> bool:
            acquired: bool = self.c_acquire(blocking=False,
                                            timeout=None)
            if acquired:
                self.c_release()

            return not acquired  # if acquired, lock was NOT held before

        @final
        @contextmanager
        def c_acquire_timeout(self,
                              blocking: bool = True,
                              timeout: Optional[float] = None) -> con.Pr.Iterator[bool]:
            result: bool = False
            try:
                result = self.c_acquire(blocking=blocking,
                                        timeout=timeout)
                yield result
            finally:
                if result:
                    self.c_release()

        @final
        def __enter__(self, /) -> bool:
            return self.c_acquire(blocking=True,
                                  timeout=None)

        @final
        def __exit__(self,
                     exc_type: Optional[Type[BaseException]],
                     exc_value: Optional[BaseException],
                     traceback: Optional[TracebackType], /) -> Literal[False]:
            # Note: Acquisition of lock should have been successful as __enter__ acquires it in a blocking manner!
            assert self.c_is_held()
            self.c_release()
            return False  # False -> do not suppress any Exceptions

    class CSLock(cs.Im.HasCSLMixin,
                 Lock,
                 Pr.CSLock):

        __f_lock: Final[Pr.Lock]

        def __init__(self,
                     csl: cs.En.CSL,
                     **kwargs: Any):
            super().__init__(csl=csl)
            self.__f_lock = self._c_init_lock(csl=csl,
                                              **kwargs)

        @final
        def _c_get_lock(self) -> Pr.Lock:
            """
            Do not change state of returned lock!
            """
            return self.__f_lock

        @final
        def c_is_held(self) -> bool:
            return self.__f_lock.c_is_held()

        @final
        def c_acquire(self,
                      blocking: bool = True,
                      timeout: Optional[float] = None) -> bool:
            return self.__f_lock.c_acquire(blocking=blocking,
                                           timeout=timeout)

        @final
        def c_release(self) -> None:
            self.__f_lock.c_release()

        @abc.abstractmethod
        def _c_init_lock(self,
                         csl: cs.En.CSL,
                         **kwargs: Any) -> Pr.Lock:
            """
            Important: If overwritten, implementation MUST guarantee that returned
            lock has a CS level not smaller than the one provided via 'csl'.
            """
            ...


class Er:
    class UnknownLockName(KeyError):
        pass


class Im:

    class CSLock(Ab.CSLock):

        def __init__(self,
                     csl: cs.En.CSL,
                     # TODO None default
                     manager: Optional[mp_mngr.SyncManager]):
            super().__init__(csl=csl,
                             manager=manager)

        @final
        def c_is_reentrant(self) -> bool:
            return False

        @final
        def _c_init_lock(self,
                         csl: cs.En.CSL,
                         **kwargs: Any) -> Pr.Lock:
            assert len(kwargs) == 1
            assert 'manager' in kwargs
            manager: Any = kwargs['manager']
            assert manager is None or isinstance(manager, mp_mngr.SyncManager)
            del kwargs['manager']
            return Ca.c_lock_from_csl(csl=csl,
                                      manager=manager)

    class CSLockableMixin(cs.Im.HasCSLMixin,
                          Pr.CSLockableMixin):

        __f_cslock_map: Final[con.Pr.Mapping[str, Pr.CSLock]]
        __f_frozen_cslock_map_view: Final[con.Pr.Mapping[str, Pr.CSLock]]

        def __init__(self,
                     *args: Any,
                     csl: cs.En.CSL,
                     # TODO None default
                     manager: Optional[mp_mngr.SyncManager],
                     cslock_keys: Optional[con.Pr.Iterable[str]] = None,
                     **kwargs: Any):
            super().__init__(*args,
                             csl=csl,
                             **kwargs)

            self.__f_cslock_map = dict()

            if cslock_keys is None:
                cslock_keys = []
            else:
                cslock_keys = list(cslock_keys)

            cslock_key_set: con.Pr.MutableSet[str] = set(cslock_keys)
            if len(cslock_key_set) != len(cslock_keys):
                raise ValueError(f"CSLock names must be unique.")

            if Co.f_DEFAULT_CSLOCK_KEY not in cslock_key_set:
                cslock_key_set.add(Co.f_DEFAULT_CSLOCK_KEY)

            # noinspection PyTypeChecker
            for cslock_name in cslock_key_set:
                self.__f_cslock_map[cslock_name] = Im.CSLock(csl=csl,
                                                             manager=manager)

            self.__f_frozen_cslock_map_view = FrozenMapping(self.__f_cslock_map,
                                                            as_view=True)

        @final
        def c_get_cslock_map(self) -> con.Pr.Mapping[str, Pr.CSLock]:
            return self.__f_frozen_cslock_map_view

        @final
        def c_get_cslock(self,
                         lock_key: str = Co.f_DEFAULT_CSLOCK_KEY) -> Pr.CSLock:
            try:
                return self.__f_cslock_map[lock_key]
            except KeyError as e:
                raise Er.UnknownLockName from e


class _Im:
    class LockSingleThread(Ab.Lock):
        """
        "Lock" mockup for single thread applications, i.e., CSL = SINGLE_THREAD
        applications. Behaves like a regular lock as long as it is always accessed
        by the same thread (including non-reentrancy). Attempting to
        re-acquire an already held lock in a blocking manner with infinite timeout
        results in a RuntimeError. Behavior in
        multithreading or multiprocessing scenarios undefined.
        """

        __f_acquired: bool

        def __init__(self) -> None:
            self.__f_acquired = False
            super().__init__()

        @final
        def c_is_reentrant(self) -> bool:
            return False

        @final
        def c_acquire(self,
                      blocking: bool = True,
                      timeout: Optional[float] = None) -> bool:
            if self.__f_acquired:
                if blocking:
                    if timeout is None:
                        raise RuntimeError("Deadlock: Attempting to perform operation which would lead to "
                                           "unbound blocking in a single thread context.")
                    else:
                        time.sleep(timeout)

                return False
            else:
                self.__f_acquired = True
                return True

        @final
        def c_release(self) -> None:
            if self.__f_acquired:
                self.__f_acquired = False
            else:
                # Error type consistent with multiprocessing package (in threading package,
                # a RuntimeError would be raised here.
                raise ValueError("Attempting to release unlocked lock.")

    class LockMultithreadAdapter(Ab.Lock):
        """
        Compatibility adapter to make threading.Lock and SyncManager.Lock instances compatible
        with the Pr.Lock protocol.
        """
        # noinspection PyProtectedMember
        __f_tlock: Final[Pr._LockThreading]

        # noinspection PyProtectedMember
        def __init__(self,
                     tlock: Pr._LockThreading):
            self.__f_tlock = tlock
            super().__init__()

        @final
        def c_is_reentrant(self) -> bool:
            return False

        @final
        def c_acquire(self,
                      blocking: bool = True,
                      timeout: Optional[float] = None) -> bool:
            if not blocking:
                return self.__f_tlock.acquire(blocking=False)
            elif timeout is None:
                return self.__f_tlock.acquire(blocking=True,
                                              timeout=-1)
            else:
                return self.__f_tlock.acquire(blocking=True,
                                              timeout=max(0.0, timeout))

        @final
        def c_release(self) -> None:
            try:
                self.__f_tlock.release()
            except RuntimeError as e:
                raise ValueError from e

    # Not used and maintained anymore
    # class LockMultiprocessAdapter(Ab.Lock):
    #     """
    #     Compatibility adapter to make multiprocessing.Lock instances compatible
    #     with the Pr.Lock protocol.
    #     """
    #
    #     # noinspection PyProtectedMember
    #     __f_mplock: Final[Pr._LockMultiprocessing]
    #
    #     # noinspection PyProtectedMember
    #     def __init__(self,
    #                  mplock: Pr._LockMultiprocessing):
    #         assert cast(str, type(mplock).__name__) == cast(str, type(multiprocessing.Lock()).__name__)  # type: ignore
    #
    #         self.__f_mplock = mplock
    #         super().__init__()
    #
    #     @final
    #     def c_is_reentrant(self) -> bool:
    #         return False
    #
    #     @final
    #     def c_acquire(self,
    #                   blocking: bool = True,
    #                   timeout: Optional[float] = None) -> bool:
    #         if not blocking:
    #             return self.__f_mplock.acquire(block=False)
    #         else:
    #             return self.__f_mplock.acquire(block=True,
    #                                            timeout=timeout)
    #
    #     @final
    #     def c_release(self) -> None:
    #         self.__f_mplock.release()


class De:
    @staticmethod
    @overload
    def d_cslock(method_or_lock_name: CALLABLE_TYPE) -> CALLABLE_TYPE:
        ...

    @staticmethod
    @overload
    def d_cslock(method_or_lock_name: str) -> Callable[[CALLABLE_TYPE], CALLABLE_TYPE]:
        ...

    @staticmethod
    def d_cslock(method_or_lock_name: Union[CALLABLE_TYPE, str]) -> Union[CALLABLE_TYPE,
                                                                          Callable[[CALLABLE_TYPE], CALLABLE_TYPE]]:
        def c_cslock_getter(cslockable: Pr.CSLockableMixin) -> Pr.CSLock:
            if isinstance(method_or_lock_name, str):
                return cslockable.c_get_cslock(lock_key=method_or_lock_name)
            else:
                # method_or_lock_name is a callable
                return cslockable.c_get_cslock()

        def d_cslock_(callable_: CALLABLE_TYPE) -> CALLABLE_TYPE:

            param_getter: Callable[[str, Tuple[Any, ...], Dict[str, Any]], ParamValue] = \
                c_get_callable_param_getter(callable_=callable_)
            self_getter: Callable[[Tuple[Any, ...], Dict[str, Any]], ParamValue] = \
                c_get_self_getter(callable_=callable_,
                                  _call_param_getter=param_getter)

            def c_get_self(args: Tuple[Any, ...],
                           kwargs: Dict[str, Any]) -> Pr.CSLockableMixin:
                self: Any = self_getter(args, kwargs).value
                assert isinstance(self, Pr.CSLockableMixin)
                return self

            try:
                # If default provided, must be False
                val: Any = param_getter('_unsafe', (), {}).value
                if val is not False:
                    raise CallableSignatureError(f"If a 'd_cslock' decorated function has a default "
                                                 f"value for '_unsafe', it must be False.")
            except ValueError:
                # At this point either no default provided or no '_unsafe' parameter exists
                try:
                    param_getter('_unsafe', (), {'_unsafe': False}).value
                except ValueError:
                    raise CallableSignatureError(f"A 'd_cslock' decorated function must take "
                                                 f"parameters 'blocking' (bool), 'timeout' (Optional[float]) "
                                                 f"and _unsafe (bool).")

            try:
                param_getter('blocking', (), {'blocking': True}).value
            except ValueError:
                raise CallableSignatureError(f"A 'd_cslock' decorated function must take "
                                             f"parameters 'blocking' (bool), 'timeout' (Optional[float]) "
                                             f"and _unsafe (bool).")

            try:
                param_getter('timeout', (), {'timeout': None}).value
            except ValueError:
                raise CallableSignatureError(f"A 'd_cslock' decorated function must take "
                                             f"parameters 'blocking' (bool), 'timeout' (Optional[float]) "
                                             f"and _unsafe (bool).")

            @wraps(callable_)
            def d_cslock__(*args: Any, **kwargs: Any) -> Any:

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

                    self: Final[Pr.CSLockableMixin] = c_get_self(args=args, kwargs=kwargs)
                    cslock: Final[Pr.CSLock] = c_cslock_getter(cslockable=self)

                    with cslock.c_acquire_timeout(blocking=blocking, timeout=timeout) as success:
                        if not success:
                            raise TimeoutError()
                        else:
                            return callable_(*args, **kwargs)
                else:
                    # _unsafe = True
                    return callable_(*args, **kwargs)

            return cast(CALLABLE_TYPE, d_cslock__)

        return (d_cslock_
                if isinstance(method_or_lock_name, str)
                else d_cslock_(method_or_lock_name))
