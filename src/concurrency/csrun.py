from __future__ import annotations

import abc
import enum
import time
from dataclasses import dataclass, field, InitVar
from typing import Protocol, final, Optional, Union, Tuple, Final, Dict, List, Any
import multiprocessing.managers as mp_mngr

from concurrency import csrwlocks, execs, cs, cslocks, csdata
import utils.types.containers as con
from utils.types import io
from utils.datastructures.frozenmapping import FrozenMapping
import utils.functional.tools as ft


"""
This module contains mostly higher level abstractions of the functionality from concurrency.execs
(which, in turn, provides higher level abstractions for executing Threads and Processes)
designed to fit better into the csl-wise design pattern of concurrency.cs* modules.
"""


RID_TYPE = Union[int]  # RID = runnable id


class En:
    class RunnerStatus(enum.IntEnum):
        NOT_STARTED = enum.auto()
        RUNNING = enum.auto()
        TERMINATING = enum.auto()
        TERMINATED = enum.auto()


class Pr:

    class CSRunnableMixin(csrwlocks.Pr.CSRWLockableMixin,
                          Protocol):

        @abc.abstractmethod
        def c_run(self) -> None:
            """
            Implementation should regularly check the status for being TERMINATING and initiate an
            orderly shutdown if the former is true.
            """
            ...

        @abc.abstractmethod
        def c_terminate(self,
                        blocking: bool = ...,
                        timeout: Optional[float] = ...,
                        _unsafe: bool = ...) -> En.RunnerStatus:
            """
            Returns previous runner status. Raises StatusError if status was NOT_STARTED before.
            Only sets to TERMINATING if runner status is < TERMINATING.
            """
            ...

        @abc.abstractmethod
        def c_get_status(self,
                         blocking: bool = ...,
                         timeout: Optional[float] = ...,
                         _unsafe: bool = ...) -> En.RunnerStatus:
            ...

        @abc.abstractmethod
        def c_is_not_started(self,
                             blocking: bool = ...,
                             timeout: Optional[float] = ...,
                             _unsafe: bool = ...) -> bool:
            ...

        @abc.abstractmethod
        def c_is_running(self,
                         blocking: bool = ...,
                         timeout: Optional[float] = ...,
                         _unsafe: bool = ...) -> bool:
            ...

        @abc.abstractmethod
        def c_is_terminating(self,
                             blocking: bool = ...,
                             timeout: Optional[float] = ...,
                             _unsafe: bool = ...) -> bool:
            ...

        @abc.abstractmethod
        def c_is_terminated(self,
                            blocking: bool = ...,
                            timeout: Optional[float] = ...,
                            _unsafe: bool = ...) -> bool:
            ...

    class CSRunnableManager(Protocol):
        """
        IMPORTANT: While CSRunnables have a csl, CSRunnableManager does not and is only
        intended for the use by one (e.g., the main) thread!
        """

        @abc.abstractmethod
        def c_exec(self,
                   csl: cs.En.CSL,
                   exec_param: Union[Im.CSRunnableExecParams,
                                     Im.CSRunnableExecDelayedParams],
                   manager: Optional[mp_mngr.SyncManager] = ...,
                   _min_sec: Optional[float] = ...,
                   _max_sec: Optional[float] = ...) -> Im.CSRunnableManagerEntry:
            ...

        @abc.abstractmethod
        def c_exec_multiple(self,
                            csl: cs.En.CSL,
                            join: bool,
                            exec_params: con.Pr.Iterable[Union[Im.CSRunnableExecParams,
                                                               Im.CSRunnableExecDelayedParams]],
                            manager: Optional[mp_mngr.SyncManager] = ...,
                            _min_sec: Optional[float] = ...,
                            _max_sec: Optional[float] = ...) -> Tuple[Im.CSRunnableManagerEntry, ...]:
            """
            Returns a CSRunnableManagerEntry for each exec_param. Each runnable ID (RID) is unique for the
            particular combination of this CSRunnableManager instance
            and this c_exec_multiple call. It effectively is a unique ID this CSRunnableManager uses to identify
            the managed runnable. IDs never get reused by the same CSRunnableManager instance.

            Note: csl controls whether the Runnable is to be executed in the same thread (effectively blocking
            until it finished), a new thread or a new process.


            All CSRunners are guaranteed to have a RunningState > NOT_STARTED upon return.
            """
            ...

        @abc.abstractmethod
        def c_update(self,
                     print_err: Union[bool, io.Pr.SupportsWrite[str]] = ...) -> None:
            """
            Remove terminated runnables from internal data structures. Also remove
            runnables which have been aborted due to exception. Raises an UncaughtException similar
            to execs.Pr.Exec in the latter case (this instance stays valid however).
            """
            ...

        @abc.abstractmethod
        def c_get_csrunnable(self,
                             rid: RID_TYPE) -> Im.CSRunnableManagerEntry:
            ...

        @abc.abstractmethod
        def c_get_csrunnables(self) -> FrozenMapping[RID_TYPE, Im.CSRunnableManagerEntry]:
            ...

        @abc.abstractmethod
        def c_join(self,
                   print_err: Union[bool, io.Pr.SupportsWrite[str]] = ...) -> None:
            ...


class Er:
    class StatusError(RuntimeError):
        pass


class Ab:

    class CSRunnableMixin(csrwlocks.Im.CSRWLockableMixin,
                          Pr.CSRunnableMixin):

        __f_status: csdata.Pr.CSData[En.RunnerStatus]

        def __init__(self,
                     *args: Any,
                     csl: cs.En.CSL,
                     csrwlock_keys: Optional[con.Pr.Iterable[str]] = None,
                     manager: Optional[mp_mngr.SyncManager] = None,
                     **kwargs: Any):

            self.__f_status = csdata.Im.CSData(csl=csl,
                                               initial_value=En.RunnerStatus.NOT_STARTED,
                                               manager=manager)

            super().__init__(*args,
                             csl=csl,
                             csrwlock_keys=csrwlock_keys,
                             manager=manager,
                             **kwargs)

        @final
        def c_run(self) -> None:
            with self.c_get_csrwlock().c_get_cswlock().c_acquire_timeout(blocking=True,
                                                                         timeout=None) as success:
                assert success
                if (status := self.__f_status.c_get(_unsafe=True)) != En.RunnerStatus.NOT_STARTED:
                    raise Er.StatusError(f"Runnable status should be '{En.RunnerStatus.NOT_STARTED.name}' "
                                         f"at this point (is '{status.name}').")

                self.__f_status.c_set(new_val=En.RunnerStatus.RUNNING,
                                      _unsafe=True)

            self._c_run()

            with self.c_get_csrwlock().c_get_cswlock().c_acquire_timeout(blocking=True,
                                                                         timeout=None) as success:
                assert success
                assert self.__f_status.c_get(_unsafe=True) in {En.RunnerStatus.RUNNING, En.RunnerStatus.TERMINATING}
                self.__f_status.c_set(new_val=En.RunnerStatus.TERMINATED,
                                      _unsafe=True)

        @abc.abstractmethod
        def _c_run(self) -> None:
            ...

        @final
        @csrwlocks.De.d_cswlock
        def c_terminate(self,
                        blocking: bool = True,
                        timeout: Optional[float] = None,
                        _unsafe: bool = False) -> En.RunnerStatus:

            if (status := self.__f_status.c_get(_unsafe=True)) == En.RunnerStatus.NOT_STARTED:
                raise Er.StatusError(f"Cannot terminate an unstarted runnable.")

            if status == En.RunnerStatus.RUNNING:
                self.__f_status.c_set(new_val=En.RunnerStatus.TERMINATING,
                                      _unsafe=True)
            else:
                assert status in {En.RunnerStatus.TERMINATING, En.RunnerStatus.TERMINATED}

            return status

        @final
        @csrwlocks.De.d_csrlock
        def c_get_status(self,
                         blocking: bool = True,
                         timeout: Optional[float] = None,
                         _unsafe: bool = False) -> En.RunnerStatus:
            return self.__f_status.c_get(_unsafe=True)

        @final
        @csrwlocks.De.d_csrlock
        def c_is_not_started(self,
                             blocking: bool = True,
                             timeout: Optional[float] = None,
                             _unsafe: bool = False) -> bool:
            return self.__f_status.c_get(_unsafe=True) == En.RunnerStatus.NOT_STARTED

        @final
        @csrwlocks.De.d_csrlock
        def c_is_running(self,
                         blocking: bool = True,
                         timeout: Optional[float] = None,
                         _unsafe: bool = False) -> bool:
            return self.__f_status.c_get(_unsafe=True) == En.RunnerStatus.RUNNING

        @final
        @csrwlocks.De.d_csrlock
        def c_is_terminating(self,
                             blocking: bool = True,
                             timeout: Optional[float] = None,
                             _unsafe: bool = False) -> bool:
            return self.__f_status.c_get(_unsafe=True) == En.RunnerStatus.TERMINATING

        @final
        @csrwlocks.De.d_csrlock
        def c_is_terminated(self,
                            blocking: bool = True,
                            timeout: Optional[float] = None,
                            _unsafe: bool = False) -> bool:
            return self.__f_status.c_get(_unsafe=True) == En.RunnerStatus.TERMINATED

    class CSLoopRunnableMixin(CSRunnableMixin):

        __f_min_loop_duration: Final[Optional[float]]

        def __init__(self,
                     *args: Any,
                     csl: cs.En.CSL,
                     min_loop_duration: Optional[float] = cslocks.Co.f_POLLING_TIMEOUT,
                     csrwlock_keys: Optional[con.Pr.Iterable[str]] = None,
                     manager: Optional[mp_mngr.SyncManager] = None,
                     **kwargs: Any):
            """
            Warning: Only set min_loop_duration to a smaller value or None if you are aware of the starvation risks! ;-)
            """
            # Loop runner only makes sense in concurrent scenario
            cs.Ca.c_are_cs_compatible(
                accessor=cs.En.CSL.MULTI_THREAD,
                accessee=csl,
                raise_if_incompatible=True,
                warn_if_redundant=False,
                msg=f"{Ab.CSLoopRunnableMixin.__name__} can only be run in a "
                    f"concurrent context (i.e., a csl larger than the provided one: '{csl.name}').")

            self.__f_min_loop_duration = (min_loop_duration
                                          if min_loop_duration is None
                                          else max(0.0, min_loop_duration))

            super().__init__(*args,
                             csl=csl,
                             csrwlock_keys=csrwlock_keys,
                             manager=manager,
                             **kwargs)

        @final
        def __c_exec_loop_and_check_terminating(self) -> bool:
            """
            :return: True iff status changed.
            """
            self._c_run_loop_iter()
            if (status := self.c_get_status()) != En.RunnerStatus.RUNNING:
                assert status == En.RunnerStatus.TERMINATING
                return True
            else:
                return False

        @final
        def _c_run(self) -> None:
            self._c_init_loop()

            if self.__f_min_loop_duration is None:
                while True:
                    if self.__c_exec_loop_and_check_terminating():
                        break
            else:
                while True:
                    start_perf_counter: float = time.perf_counter()
                    if self.__c_exec_loop_and_check_terminating():
                        break
                    else:
                        time.sleep(max(0.0, self.__f_min_loop_duration - (time.perf_counter() - start_perf_counter)))

            self._c_cleanup_loop()

        @abc.abstractmethod
        def _c_init_loop(self) -> None:
            ...

        @abc.abstractmethod
        def _c_cleanup_loop(self) -> None:
            ...

        @abc.abstractmethod
        def _c_run_loop_iter(self) -> None:
            """
            Executed at least once. Call self.c_terminate() in order to exit loop.
            """
            ...


class Im:

    class CSRunnableManager(Pr.CSRunnableManager):

        __f_csrunnables: Final[Dict[RID_TYPE, Im.CSRunnableManagerEntry]]
        __f_exec: Final[execs.Pr.Exec]
        __f_next_rid: int

        def __init__(self) -> None:
            self.__f_csrunnables = dict()
            self.__f_exec = execs.Im.Exec()
            self.__f_next_rid = 0

        @final
        def c_exec(self,
                   csl: cs.En.CSL,
                   exec_param: Union[Im.CSRunnableExecParams,
                                     Im.CSRunnableExecDelayedParams],
                   manager: Optional[mp_mngr.SyncManager] = None,
                   _min_sec: Optional[float] = None,
                   _max_sec: Optional[float] = None) -> Im.CSRunnableManagerEntry:

            result: Tuple[Im.CSRunnableManagerEntry, ...] = \
                self.c_exec_multiple(csl=csl,
                                     join=False,
                                     exec_params=(exec_param,),
                                     manager=manager,
                                     _min_sec=_min_sec,
                                     _max_sec=_max_sec)

            assert len(result) == 1
            return result[0]

        # noinspection PyArgumentList
        @final
        def c_exec_multiple(self,
                            csl: cs.En.CSL,
                            join: bool,
                            exec_params: con.Pr.Iterable[Union[Im.CSRunnableExecParams,
                                                               Im.CSRunnableExecDelayedParams]],
                            manager: Optional[mp_mngr.SyncManager] = None,
                            _min_sec: Optional[float] = None,
                            _max_sec: Optional[float] = None) -> Tuple[Im.CSRunnableManagerEntry, ...]:

            exec_params = con.Ca.c_to_seq(obj=exec_params)
            num_execs: Final[int] = len(exec_params)

            for i in range(num_execs):
                cs.Ca.c_are_cs_compatible(
                    accessor=csl,
                    accessee=exec_params[i].csrunnable,
                    raise_if_incompatible=True,
                    warn_if_redundant=False,
                    msg=f"A {Pr.CSRunnableMixin.__name__} of csl "
                        f"'{exec_params[i].csrunnable.c_get_csl().name}' "
                        f"cannot be run in a csl context with larger concurrency level (here: "
                        f"'{csl.name}').")

            exec_params_: Tuple[Union[execs.Im.ExecParams,
                                      execs.Im.ExecDelayedParams], ...] = tuple(
                (execs.Im.ExecDelayedParams(
                    func_or_obj_func=(exec_param.csrunnable,
                                      exec_param.csrunnable.c_run.__name__),
                    params=None,
                    join=exec_param.join,
                    print_err=exec_param.print_err,
                    runner_name=exec_param.csrunner_name,
                    daemon=exec_param.daemon,
                    pre_delay=exec_param.pre_delay,
                    in_delay=exec_param.in_delay,
                    post_delay=exec_param.post_delay,
                    min_sec=exec_param.min_sec,
                    max_sec=exec_param.max_sec)
                 if isinstance(exec_param, Im.CSRunnableExecDelayedParams)
                 else execs.Im.ExecParams(
                    func_or_obj_func=(exec_param.csrunnable,
                                      exec_param.csrunnable.c_run.__name__),
                    params=None,
                    join=exec_param.join,
                    print_err=exec_param.print_err,
                    runner_name=exec_param.csrunner_name,
                    daemon=exec_param.daemon))
                for exec_param in exec_params
            )
            assert len(exec_params_) == num_execs

            runners: Tuple[Optional[Union[execs.Im.ExceptionSafeThread,
                                          execs.Im.ExceptionSafeProcess]], ...] = \
                self.__f_exec.c_exec_multiple(csl=csl,
                                              join=join,
                                              exec_params=exec_params_,
                                              manager=manager,
                                              _min_sec=_min_sec,
                                              _max_sec=_max_sec)

            for rid_offset in range(num_execs):
                ft.c_poll_condition(
                    condition_check=lambda: exec_params[rid_offset].csrunnable.c_get_status() > En.RunnerStatus.NOT_STARTED,  # type: ignore
                    params=None
                )

            # noinspection PyUnreachableCode
            if __debug__:
                assert len(runners) == num_execs
                assert all(exec_params_[rid_offset].join or csl > cs.En.CSL.SINGLE_THREAD
                           for rid_offset in range(num_execs))  # otherwise __f_exec.c_exec_multiple should have raised an error
                assert all((not join and not exec_params_[rid_offset].join
                            or exec_params[rid_offset].csrunnable.c_is_terminated())
                           for rid_offset in range(num_execs))
                assert all(exec_params[rid_offset].csrunnable.c_get_status() > En.RunnerStatus.NOT_STARTED
                           for rid_offset in range(num_execs))

            result: Tuple[Im.CSRunnableManagerEntry, ...] = tuple(
                Im.CSRunnableManagerEntry(
                    csrunnable=exec_params[rid_offset].csrunnable,
                    runner=runners[rid_offset],
                    rid=self.__f_next_rid + rid_offset,
                    csl_=csl)
                for rid_offset in range(num_execs))

            # noinspection PyUnreachableCode
            if __debug__:
                assert len(result) == num_execs
                assert all(self.__f_next_rid + rid_offset not in self.__f_csrunnables
                           for rid_offset in range(num_execs))

            if csl > cs.En.CSL.SINGLE_THREAD:
                for rid_offset in range(num_execs):
                    assert result[rid_offset].rid == self.__f_next_rid + rid_offset
                    if result[rid_offset].csrunnable.c_get_status() < En.RunnerStatus.TERMINATED:
                        self.__f_csrunnables[result[rid_offset].rid] = result[rid_offset]

            self.__f_next_rid = self.__f_next_rid + num_execs
            return result

        @final
        def c_update(self,
                     print_err: Union[bool, io.Pr.SupportsWrite[str]] = True) -> None:
            runners_with_uncaught: List[Union[execs.Im.ExceptionSafeThread,
                                              execs.Im.ExceptionSafeProcess]] = list()
            remove_rid: con.Pr.MutableSet[RID_TYPE] = set()
            for rid in self.__f_csrunnables:
                assert self.__f_csrunnables[rid].csl > cs.En.CSL.SINGLE_THREAD
                # SINGLE_THREAD case is immediately executed and not stored internally

                runner: Optional[Union[execs.Im.ExceptionSafeThread, execs.Im.ExceptionSafeProcess]] = \
                    self.__f_csrunnables[rid].runner
                assert runner is not None
                # SINGLE_THREAD case is immediately executed and not stored internally

                if runner.c_get_exception_info(join_first=False) is not None:
                    runners_with_uncaught.append(runner)
                    remove_rid.add(rid)
                elif self.__f_csrunnables[rid].csrunnable.c_is_terminated():
                    remove_rid.add(rid)

            # noinspection PyTypeChecker
            for rid in remove_rid:
                del self.__f_csrunnables[rid]

            self.__f_exec.c_join_collect_reraise(runners=runners_with_uncaught,
                                                 print_err=print_err)

        @final
        def c_get_csrunnable(self,
                             rid: RID_TYPE) -> Im.CSRunnableManagerEntry:
            return self.__f_csrunnables[rid]

        @final
        def c_get_csrunnables(self) -> FrozenMapping[RID_TYPE, Im.CSRunnableManagerEntry]:
            return FrozenMapping(mapping=self.__f_csrunnables,
                                 as_view=True)

        @final
        def c_join(self,
                   print_err: Union[bool, io.Pr.SupportsWrite[str]] = True) -> None:
            uncaught_exception: Optional[execs.Er.UncaughtException] = None
            try:
                self.__f_exec.c_join(print_err=False)
            except execs.Er.UncaughtException as e:
                uncaught_exception = e

            uncaught_exception_2: Optional[execs.Er.UncaughtException] = None
            try:
                self.c_update(print_err=print_err)
            except execs.Er.UncaughtException as e_2:
                uncaught_exception_2 = e_2

            assert (uncaught_exception is None) == (uncaught_exception_2 is None)
            assert (uncaught_exception is None
                    or (uncaught_exception_2 is not None
                        and uncaught_exception.UNCAUGHT_EXCEPTION_INFOS == uncaught_exception_2.UNCAUGHT_EXCEPTION_INFOS))
            if uncaught_exception is not None:
                raise uncaught_exception

    @dataclass(frozen=True)
    class CSRunnableManagerEntry:
        csrunnable: Pr.CSRunnableMixin
        csl: cs.En.CSL = field(init=False)
        runner: Optional[Union[execs.Im.ExceptionSafeThread, execs.Im.ExceptionSafeProcess]]
        # runner is set to None if csl of csrunner is SINGLE_THREAD
        rid: RID_TYPE  # = runnable id
        csl_: InitVar[Optional[cs.En.CSL]] = None

        def __post_init__(self,
                          csl_: Optional[cs.En.CSL]) -> None:
            if self.runner is None:
                if csl_ is not None and csl_ != cs.En.CSL.SINGLE_THREAD:
                    raise ValueError
                super().__setattr__("csl", cs.En.CSL.SINGLE_THREAD)
            elif isinstance(self.runner, execs.Im.ExceptionSafeThread):
                if csl_ is not None and csl_ != cs.En.CSL.MULTI_THREAD:
                    raise ValueError
                super().__setattr__("csl", cs.En.CSL.MULTI_THREAD)
            else:
                assert isinstance(self.runner, execs.Im.ExceptionSafeProcess)
                if csl_ is not None and csl_ != cs.En.CSL.MULTI_PROCESS:
                    raise ValueError
                super().__setattr__("csl", cs.En.CSL.MULTI_PROCESS)
            assert isinstance(self.csl, cs.En.CSL)

    @dataclass(frozen=True)
    class CSRunnableExecParams:
        csrunnable: Pr.CSRunnableMixin
        join: bool
        csrunner_name: Optional[str] = None
        print_err: Union[bool, io.Pr.SupportsWrite[str]] = True
        # 'print_err' is ignored if join=False
        daemon: Optional[bool] = None

    @dataclass(frozen=True)
    class CSRunnableExecDelayedParams(CSRunnableExecParams):
        pre_delay: Optional[float] = None
        in_delay: Optional[float] = None
        post_delay: Optional[float] = None
        min_sec: Optional[float] = None
        max_sec: Optional[float] = None
