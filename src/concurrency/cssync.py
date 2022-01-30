"""
Higher level synchronization tools based on cs(rw)locks and csdata.
"""
import abc
import time
from typing import Optional, runtime_checkable, Protocol, Final, cast, final, Tuple

from concurrency import cs, cslocks, csdata
import utils.types.containers as con
from concurrency import managers as mngr


class Pr:
    @runtime_checkable
    class CSCondition(cs.Pr.HasCSLMixin,
                      Protocol):
        """
        Note: threading/multiprocessing Condition also implements a locking interface on the underlying resource
        lock. This functionality is omitted here to keep the interface minimal.

        Note that the resource lock must be held when calling c_wait, c_notify and c_notify_all (is checked) and
        that lock must not be concurrently released by some other thread/process
        during he call of said methods (cannot be checked, if the former happens, the behaviour is undefined).
        """

        @abc.abstractmethod
        def c_get_resource_cslock(self) -> cslocks.Pr.CSLock:
            ...

        @abc.abstractmethod
        def c_get_num_waiting(self) -> int:
            ...

        @abc.abstractmethod
        def c_wait(self,
                   blocking: bool = ...,
                   timeout: Optional[float] = ...) -> bool:
            """
            Note: The timeout only refers to the waiting time until a call to `c_notify(_all)` occurs
            which wakes this thread. Afterwards the underlying resource lock is being re-acquired
            in a blocking manner without a timeout in order to ensure that `c_wait` exits
            while holding the resource lock. This reacquiring can therefore block indefinitely if some other
            thread/process acquired it in the meantime.

            Returns True if and only if timeout did not fire.
            """
            ...

        @abc.abstractmethod
        def c_notify(self,
                     n: int = ...) -> None:
            """
            Wakes max(0, min(n, <num_waiting_threads/processes>)) threads or processes.
            """
            ...

        @abc.abstractmethod
        def c_notify_all(self) -> None:
            ...


class Im:

    class CSCondition(mngr.Im.PicklableSyncManagedMixin,
                      Pr.CSCondition):

        __f_waiter_lock_id: Final[csdata.Pr.CSData[int]]
        __f_waiter_locks_mlseq: Final[csdata.Pr.CSMutableLengthSequence[Tuple[int, cslocks.Pr.CSLock]]]
        __f_resource_lock: Final[cslocks.Pr.CSLock]

        def __init__(self,
                     csl: cs.En.CSL,
                     resource_lock: Optional[cslocks.Pr.CSLock] = None,
                     manager: Optional[mngr.Im.PicklableSyncManager] = None):

            if resource_lock is None:
                resource_lock = cslocks.Im.CSLock(csl=csl,
                                                  manager=manager)
            else:
                cs.Ca.c_are_cs_compatible(accessor=csl,
                                          accessee=resource_lock,
                                          raise_if_incompatible=True,
                                          warn_if_redundant=False)
                if resource_lock.c_is_reentrant():
                    raise ValueError(f"'{Im.CSCondition.__name__}' currently only work on non-reentrant "
                                     f"resource locks.")

            assert cs.Ca.c_are_cs_compatible(accessor=csl,
                                             accessee=resource_lock) and not resource_lock.c_is_reentrant()

            self.__f_waiter_locks_mlseq = csdata.Im.CSMutableLengthSequence(
                csl=csl,
                initial_values=cast(con.Pr.Sequence[Tuple[int, cslocks.Pr.CSLock]], ()),
                manager=manager)
            self.__f_resource_lock = resource_lock
            self.__f_waiter_lock_id = csdata.Im.CSData(
                csl=csl,
                initial_value=0,
                manager=manager
            )

            super().__init__(csl=csl,
                             picklable_sync_manager=manager)

        @final
        def c_get_num_waiting(self) -> int:
            return self.__f_waiter_locks_mlseq.c_len()

        @final
        def c_get_resource_cslock(self) -> cslocks.Pr.CSLock:
            return self.__f_resource_lock

        @final
        def c_wait(self,
                   blocking: bool = True,
                   timeout: Optional[float] = None) -> bool:

            self.__c_check_is_resource_held()
            if blocking and self.c_get_csl() == cs.En.CSL.SINGLE_THREAD:
                if timeout is None:
                    raise RuntimeError("Deadlock: Attempting to perform operation which would lead to "
                                       "unbound blocking in a single thread context.")
                else:
                    # Mimic behaviour in a concurrent context
                    self.__f_resource_lock.c_release()
                    time.sleep(timeout)
                    self.__f_resource_lock.c_acquire()
                return False

            new_waiter_lock: Final[cslocks.Pr.CSLock] = cslocks.Im.CSLock(csl=self.c_get_csl(),
                                                                          manager=self._c_get_manager())

            assert cs.Ca.c_are_cs_compatible(accessor=self.c_get_csl(),
                                             accessee=new_waiter_lock) and not new_waiter_lock.c_is_reentrant()

            new_waiter_lock.c_acquire()
            waiter_id: Final[int] = self.__f_waiter_lock_id.c_apply(func=lambda old_val: (old_val + 1, old_val))
            with self.__f_waiter_locks_mlseq.c_get_csrwlock().c_get_cswlock():
                new_index: Final[int] = self.__f_waiter_locks_mlseq.c_len(_unsafe=True)
                self.__f_waiter_locks_mlseq.c_append(value=(waiter_id, new_waiter_lock),
                                                     _unsafe=True)

            self.__f_resource_lock.c_release()

            result: Final[bool] = new_waiter_lock.c_acquire(blocking=blocking,
                                                            timeout=timeout)  # Blocks until notify
            if not result:
                with self.__f_waiter_locks_mlseq.c_get_csrwlock().c_get_cswlock():
                    # Entries get removed from the left -> current index of waiter must be <= new_index
                    # if it is not removed concurrently due to a notify call
                    for idx in range(0, min(new_index + 1, self.__f_waiter_locks_mlseq.c_len(_unsafe=True))):
                        if self.__f_waiter_locks_mlseq.c_get(index=idx,
                                                             _unsafe=True)[0] == waiter_id:
                            self.__f_waiter_locks_mlseq.c_delete(index=idx,
                                                                 _unsafe=True)
                            break

            assert all(item[0] != waiter_id
                       for item in self.__f_waiter_locks_mlseq.c_get(index=slice(0, None)))

            # 'waiter' lock should not be in list anymore unless waiting was not successful
            # and gets garbage collected after we return from this method

            self.__f_resource_lock.c_acquire()
            return result

        @final
        def c_notify(self,
                     n: int = 1) -> None:

            if n > 0:
                self.__c_notify(n=n)

        def __c_check_is_resource_held(self) -> None:
            if not self.__f_resource_lock.c_is_held():
                raise ValueError(f"Resource lock must be held at this point.")

        def __c_notify(self,
                       n: Optional[int]) -> None:
            self.__c_check_is_resource_held()

            assert n is None or n > 0
            waiter_lock: cslocks.Pr.CSLock
            # This should be concurrency safe as we atomically retrieve and delete elements from sequence
            for item in con.Ca.c_to_itbl(
                    obj=self.__f_waiter_locks_mlseq.c_apply(index=slice(0, n),  # Should be safe even if len < n
                                                            func=lambda old_vals: ((), old_vals))):
                waiter_lock = item[1]
                assert waiter_lock.c_is_held()
                waiter_lock.c_release()
                # The above should not raise a ValueError as lock is appended to list AFTER first acquire

        @final
        def c_notify_all(self) -> None:
            self.__c_notify(n=None)
