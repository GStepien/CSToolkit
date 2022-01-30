from __future__ import annotations

import abc
import multiprocessing as mp
import os
import sys
import threading as th
import time
from dataclasses import dataclass, field
from functools import wraps
import multiprocessing.managers as mp_mngr
from traceback import StackSummary, extract_tb
from typing import Any, cast, Callable, Tuple, Dict, Optional, Union, List, Final, Protocol, final, Type

import concurrency._raw_csdata as rcsd
from concurrency import cs, cslocks
import utils.types.containers as con
from utils.functional.tools import c_normalize_params, c_normalize_func
from utils.testing.testasserts import de_assert_duration
from utils.types import io

from utils.types.typevars import CALLABLE_TYPE


class De:
    @staticmethod
    def de_delay(sec: float) -> Callable[[CALLABLE_TYPE], CALLABLE_TYPE]:
        def de_delay_(func: CALLABLE_TYPE) -> CALLABLE_TYPE:
            @wraps(func)
            def c_wrapped_func(*args: Any, **kwargs: Any) -> Any:
                time.sleep(sec)
                return func(*args, **kwargs)

            return cast(CALLABLE_TYPE, c_wrapped_func)

        return de_delay_


class Er:
    class UncaughtException(RuntimeError):
        UNCAUGHT_EXCEPTION_INFOS: Final[Tuple[Im.ExceptionInfo, ...]]

        def __init__(self,
                     msg: str,
                     uncaught_exception_infos: con.Pr.Iterable[Im.ExceptionInfo]):
            self.UNCAUGHT_EXCEPTION_INFOS = tuple(uncaught_exception_infos)
            assert len(self.UNCAUGHT_EXCEPTION_INFOS)
            super().__init__(msg)


class Ca:
    @staticmethod
    def _c_exec_func_or_obj_func(func_or_obj_func: Union[Callable[..., Any], Tuple[Any, str]],
                                 params: Optional[Tuple[Optional[con.Pr.Iterable[Any]],
                                                        Optional[con.Pr.Mapping[str, Any]]]],
                                 in_delay: Optional[float]) -> None:
        if in_delay is not None and in_delay > 0:
            time.sleep(in_delay)

        args, kwargs = c_normalize_params(params=params)
        func = c_normalize_func(func_or_obj_func=func_or_obj_func)

        func(*args, **kwargs)

    @staticmethod
    def c_exec(csl: cs.En.CSL,
               exec_: Pr.Exec,
               exec_param: Union[Im.ExecParams,
                                 Im.ExecDelayedParams],
               manager: Optional[mp_mngr.SyncManager] = None,
               # Union above not necessary, but more explicit this way
               _min_sec: Optional[float] = None,
               _max_sec: Optional[float] = None) -> Optional[Union[Im.ExceptionSafeThread,
                                                                   Im.ExceptionSafeProcess]]:
        result: Tuple[Optional[Union[Im.ExceptionSafeThread,
                                     Im.ExceptionSafeProcess]], ...] = \
            Ca.c_exec_multiple(csl=csl,
                               exec_=exec_,
                               join=False,  # For single exec param, join param in exec_param is enough
                               exec_params=(exec_param,),
                               manager=manager,
                               _min_sec=_min_sec,
                               _max_sec=_max_sec)
        assert len(result) == 1
        return result[0]

    @staticmethod
    def c_exec_multiple(csl: cs.En.CSL,
                        exec_: Pr.Exec,
                        join: bool,
                        exec_params: con.Pr.Iterable[Union[Im.ExecParams,
                                                           Im.ExecDelayedParams]],
                        manager: Optional[mp_mngr.SyncManager] = None,
                        # Union above not necessary, but more explicit this way
                        _min_sec: Optional[float] = None,
                        _max_sec: Optional[float] = None) -> Tuple[Optional[Union[Im.ExceptionSafeThread,
                                                                                  Im.ExceptionSafeProcess]], ...]:

        exec_params = con.Ca.c_to_seq(obj=exec_params)
        result: List[Optional[Union[Im.ExceptionSafeThread,
                                    Im.ExceptionSafeProcess]]] = list()

        @de_assert_duration(min_sec=_min_sec,
                            max_sec=_max_sec)
        def c_exec_delayed_multiple_() -> None:
            for exec_param in exec_params:
                min_sec_local: Optional[float]
                max_sec_local: Optional[float]
                pre_delay_local: Optional[float]
                in_delay_local: Optional[float]
                post_delay_local: Optional[float]

                if isinstance(exec_param, Im.ExecDelayedParams):
                    min_sec_local = exec_param.min_sec
                    max_sec_local = exec_param.max_sec
                    pre_delay_local = exec_param.pre_delay
                    in_delay_local = exec_param.in_delay
                    post_delay_local = exec_param.post_delay
                else:
                    min_sec_local = None
                    max_sec_local = None
                    pre_delay_local = None
                    in_delay_local = None
                    post_delay_local = None

                @de_assert_duration(min_sec=min_sec_local,
                                    max_sec=max_sec_local)
                def c_exec_delayed_multiple__() -> None:
                    if pre_delay_local is not None and pre_delay_local > 0:
                        time.sleep(pre_delay_local)

                    # noinspection PyProtectedMember
                    result.append(exec_._c_exec(
                        csl=csl,
                        func=Ca._c_exec_func_or_obj_func,
                        params=((),
                                {'func_or_obj_func': exec_param.func_or_obj_func,
                                 'params': exec_param.params,
                                 'in_delay': in_delay_local}),
                        join=exec_param.join,
                        manager=manager,
                        print_err=exec_param.print_err,
                        runner_name=exec_param.runner_name,
                        daemon=exec_param.daemon))

                    if post_delay_local is not None and post_delay_local > 0:
                        time.sleep(post_delay_local)

                c_exec_delayed_multiple__()

            if join:
                exec_.c_join()

        c_exec_delayed_multiple_()
        assert len(result) == len(exec_params)
        return tuple(result)


class Pr:
    class Exec(Protocol):

        @abc.abstractmethod
        def c_exec(self,
                   csl: cs.En.CSL,
                   exec_param: Union[Im.ExecParams,
                                     Im.ExecDelayedParams],
                   # Union above not necessary, but more explicit this way
                   manager: Optional[mp_mngr.SyncManager] = ...,
                   _min_sec: Optional[float] = ...,
                   _max_sec: Optional[float] = ...) -> Optional[Union[Im.ExceptionSafeThread,
                                                                      Im.ExceptionSafeProcess]]:
            ...

        @abc.abstractmethod
        def c_exec_multiple(self,
                            csl: cs.En.CSL,
                            join: bool,
                            exec_params: con.Pr.Iterable[Union[Im.ExecParams,
                                                               Im.ExecDelayedParams]],
                            manager: Optional[mp_mngr.SyncManager] = ...,
                            # Union above not necessary, but more explicit this way
                            _min_sec: Optional[float] = ...,
                            _max_sec: Optional[float] = ...) -> Tuple[Optional[Union[Im.ExceptionSafeThread,
                                                                                     Im.ExceptionSafeProcess]], ...]:
            ...

        @abc.abstractmethod
        def _c_exec(self,
                    csl: cs.En.CSL,
                    func: Callable[..., Any],
                    params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]],
                    join: bool,
                    runner_name: Optional[str],
                    manager: Optional[mp_mngr.SyncManager],
                    print_err: Union[bool, io.Pr.SupportsWrite[str]] = ...,
                    *,
                    daemon: Optional[bool]) -> Optional[Union[Im.ExceptionSafeThread,
                                                              Im.ExceptionSafeProcess]]:
            ...

        @abc.abstractmethod
        def c_get_runners(self) -> Tuple[Union[Im.ExceptionSafeThread, Im.ExceptionSafeProcess], ...]:
            ...

        @staticmethod
        @abc.abstractmethod
        def c_join_collect_reraise(runners: List[Union[Im.ExceptionSafeThread, Im.ExceptionSafeProcess]],
                                   print_err: Union[bool, io.Pr.SupportsWrite[str]] = ...) -> None:
            """
            Should empty provided list.
            """
            ...

        @abc.abstractmethod
        def c_join(self,
                   print_err: Union[bool, io.Pr.SupportsWrite[str]] = ...) -> None:
            ...


class Im:
    @dataclass(frozen=True)
    class ExceptionInfo:
        runner_name: Optional[str]
        process_id: int
        """
        os.getpid()
        """
        thread_id: int
        """
        th.get_ident()
        """
        exception: BaseException = field(compare=False)
        exception_type: Type[BaseException] = field(init=False)
        stack_summary: StackSummary

        def __post_init__(self) -> None:
            super().__setattr__('exception_type', type(self.exception))

        def __str__(self) -> str:
            return (f"START: Exception occurred\n" +
                    f"\tException: {repr(self.exception)}\n" +
                    f"\tRunner name: {self.runner_name}\n" +
                    f"\tProcess id: {self.process_id}\n" +
                    f"\tThread id: {self.thread_id}\n" +
                    f"\tStack info:\n" +
                    "".join((f"\t{item}"
                             for item in self.stack_summary.format())) +
                    f"END: Exception occurred")
        
    class _ExceptionHandler(cslocks.Im.CSLockableMixin):
        __f_exception_info: Final[rcsd.Pr.Value[Optional[Im.ExceptionInfo]]]
        __f_reraise: Final[bool]

        def __init__(self,
                     csl: cs.En.CSL,
                     manager: Optional[mp_mngr.SyncManager],
                     reraise: bool = False):
            self.__f_exception_info = rcsd.Ca.c_to_cs_value(csl=csl,
                                                            init_value=None,
                                                            manager=manager)
            self.__f_reraise = reraise

            super().__init__(csl=csl,
                             manager=manager)

        # noinspection PyUnusedLocal
        @cslocks.De.d_cslock
        def __c_set_exception_info(self,
                                   e: Im.ExceptionInfo,
                                   blocking: bool = True,
                                   timeout: Optional[float] = None,
                                   _unsafe: bool = False) -> None:
            self.__f_exception_info.value = e

        # noinspection PyUnusedLocal
        @cslocks.De.d_cslock
        def __c_get_exception_info(self,
                                   blocking: bool = True,
                                   timeout: Optional[float] = None,
                                   _unsafe: bool = False) -> Optional[Im.ExceptionInfo]:
            return self.__f_exception_info.value

        def c_run(self,
                  runner_name: Optional[str],
                  super_run: Callable[[], None]) -> None:
            # noinspection PyBroadException
            try:
                super_run()
            except BaseException as e:
                self.__c_set_exception_info(e=Im.ExceptionInfo(runner_name=runner_name,
                                                               process_id=os.getpid(),
                                                               thread_id=th.get_ident(),
                                                               exception=e,
                                                               stack_summary=extract_tb(sys.exc_info()[2])),
                                            blocking=True,
                                            timeout=None)
                if self.__f_reraise:
                    raise e

        def c_get_exception_info(self) -> Optional[Im.ExceptionInfo]:
            return self.__c_get_exception_info(blocking=True,
                                               timeout=None)

    class ExceptionSafeThread(th.Thread):

        __f_exception_handler: Final[Im._ExceptionHandler]

        def __init__(self,
                     target: Callable[..., Any],
                     args: Tuple[Any, ...],
                     kwargs: Dict[str, Any],
                     reraise: bool = False,
                     group: None = None,
                     name: Optional[str] = None,
                     *,
                     daemon: Optional[bool] = None):

            self.__f_exception_handler = Im._ExceptionHandler(csl=cs.En.CSL.MULTI_THREAD,
                                                              manager=None,
                                                              reraise=reraise)

            super().__init__(group=group,
                             target=target,
                             name=name,
                             args=args,
                             kwargs=kwargs,
                             daemon=daemon)

        def run(self) -> None:
            self.__f_exception_handler.c_run(runner_name=self.name,
                                             super_run=super().run)

        def c_get_exception_info(self,
                                 join_first: bool = True) -> Optional[Im.ExceptionInfo]:
            """
            Note: Use traceback.print_list on the 'stack_summary' entry of the result (if the latter is not None)
            in order to print a stacktrace to console.
            """
            if join_first:
                self.join()
            return self.__f_exception_handler.c_get_exception_info()

    class ExceptionSafeProcess(mp.Process):

        __f_exception_handler: Final[Im._ExceptionHandler]

        def __init__(self,
                     target: Callable[..., Any],
                     args: Tuple[Any, ...],
                     kwargs: Dict[str, Any],
                     manager: mp_mngr.SyncManager,
                     reraise: bool = False,
                     group: None = None,
                     name: Optional[str] = None,
                     *,
                     daemon: Optional[bool] = None):

            self.__f_exception_handler = Im._ExceptionHandler(csl=cs.En.CSL.MULTI_PROCESS,
                                                              manager=manager,
                                                              reraise=reraise)

            super().__init__(group=group,
                             target=target,
                             name=name,
                             args=args,
                             kwargs=kwargs,
                             daemon=daemon)

        def run(self) -> None:
            self.__f_exception_handler.c_run(runner_name=self.name,
                                             super_run=super().run)

        def c_get_exception_info(self,
                                 join_first: bool = True) -> Optional[Im.ExceptionInfo]:
            """
            Note: Use traceback.print_list on the 'stack_summary' entry of the result (if the latter is not None)
            in order to print a stacktrace to console.
            """
            if join_first:
                self.join()
            return self.__f_exception_handler.c_get_exception_info()

    class Exec(Pr.Exec):
        __f_runners: Final[List[Union[Im.ExceptionSafeThread, Im.ExceptionSafeProcess]]]

        def __init__(self) -> None:
            self.__f_runners = list()

        @final
        def c_exec(self,
                   csl: cs.En.CSL,
                   exec_param: Union[Im.ExecParams,
                                     Im.ExecDelayedParams],
                   # Union above not necessary, but more explicit this way
                   manager: Optional[mp_mngr.SyncManager] = None,
                   _min_sec: Optional[float] = None,
                   _max_sec: Optional[float] = None) -> Optional[Union[Im.ExceptionSafeThread,
                                                                       Im.ExceptionSafeProcess]]:
            """
            Method included for convenience.
            """
            return Ca.c_exec(csl=csl,
                             exec_=self,
                             exec_param=exec_param,
                             manager=manager,
                             _min_sec=_min_sec,
                             _max_sec=_max_sec)

        @final
        def c_exec_multiple(self,
                            csl: cs.En.CSL,
                            join: bool,
                            exec_params: con.Pr.Iterable[Union[Im.ExecParams,
                                                               Im.ExecDelayedParams]],
                            manager: Optional[mp_mngr.SyncManager] = None,
                            # Union above not necessary, but more explicit this way
                            _min_sec: Optional[float] = None,
                            _max_sec: Optional[float] = None) -> Tuple[Optional[Union[Im.ExceptionSafeThread,
                                                                                      Im.ExceptionSafeProcess]], ...]:
            """
            Method included for convenience.
            """
            return Ca.c_exec_multiple(csl=csl,
                                      exec_=self,
                                      join=join,
                                      exec_params=exec_params,
                                      manager=manager,
                                      _min_sec=_min_sec,
                                      _max_sec=_max_sec)

        @final
        def _c_exec(self,
                    csl: cs.En.CSL,
                    func: Callable[..., Any],
                    params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]],
                    join: bool,
                    runner_name: Optional[str],
                    manager: Optional[mp_mngr.SyncManager],
                    print_err: Union[bool, io.Pr.SupportsWrite[str]] = True,
                    *,
                    daemon: Optional[bool]) -> Optional[Union[Im.ExceptionSafeThread,
                                                              Im.ExceptionSafeProcess]]:

            args, kwargs = c_normalize_params(params=params)
            result: Optional[Union[Im.ExceptionSafeThread,
                                   Im.ExceptionSafeProcess]]

            if csl == cs.En.CSL.SINGLE_THREAD:
                if not join:
                    raise ValueError("'join' cannot be False for 'csl == CSL.SINGLE_THREAD'.")

                try:
                    func(*args, **kwargs)
                except BaseException as e:
                    exc_info: Final[Im.ExceptionInfo] = Im.ExceptionInfo(
                        runner_name=runner_name,
                        process_id=os.getpid(),
                        thread_id=th.get_ident(),
                        exception=e,
                        stack_summary=extract_tb(sys.exc_info()[2]))

                    Im.Exec._c_print_exc_info(exc_info=exc_info,
                                              print_err=print_err)

                    raise Er.UncaughtException(
                        msg=f"Uncaught exception occurred during single threaded execution.",
                        uncaught_exception_infos=(exc_info,))
                result = None
            elif csl == cs.En.CSL.MULTI_THREAD:
                thread = Im.ExceptionSafeThread(target=func,
                                                args=args,
                                                kwargs=kwargs,
                                                reraise=False,
                                                name=runner_name,
                                                daemon=daemon)
                thread.start()
                if join:
                    self.c_join_collect_reraise(runners=[thread],
                                                print_err=print_err)
                else:
                    self.__f_runners.append(thread)
                result = thread

            elif csl == cs.En.CSL.MULTI_PROCESS:
                if manager is None:
                    raise ValueError("A manager must be provided for process spawning.")
                process = Im.ExceptionSafeProcess(target=func,
                                                  args=args,
                                                  kwargs=kwargs,
                                                  manager=manager,
                                                  reraise=False,
                                                  name=runner_name,
                                                  daemon=daemon)
                process.start()
                if join:
                    self.c_join_collect_reraise(runners=[process],
                                                print_err=print_err)
                else:
                    self.__f_runners.append(process)
                result = process

            else:
                raise ValueError(f"Unknown CSL '{csl.name}'")
            return result

        @final
        def c_get_runners(self) -> Tuple[Union[Im.ExceptionSafeThread, Im.ExceptionSafeProcess], ...]:
            return tuple(self.__f_runners)

        @staticmethod
        @final
        def _c_print_exc_info(exc_info: Im.ExceptionInfo,
                              print_err: Union[bool, io.Pr.SupportsWrite[str]]) -> None:

            print_: Final[bool] = not isinstance(print_err, bool) or print_err
            if print_:
                print_file: Final[io.Pr.SupportsWrite[str]] = (sys.stderr
                                                               if isinstance(print_err, bool)
                                                               else print_err)
                print(exc_info, file=print_file)

        @staticmethod
        @final
        def c_join_collect_reraise(runners: List[Union[Im.ExceptionSafeThread, Im.ExceptionSafeProcess]],
                                   print_err: Union[bool, io.Pr.SupportsWrite[str]] = True) -> None:

            runner: Union[Im.ExceptionSafeThread, Im.ExceptionSafeProcess]
            uncaught_exception_infos: List[Im.ExceptionInfo] = list()

            for _ in range(len(runners)):
                runner = runners.pop()
                if (exc_info := runner.c_get_exception_info(join_first=True)) is not None:
                    Im.Exec._c_print_exc_info(exc_info=exc_info,
                                              print_err=print_err)
                    uncaught_exception_infos.append(exc_info)
            assert len(runners) == 0
            if len(uncaught_exception_infos) > 0:
                raise Er.UncaughtException(msg=f"At least one exception occurred during joining.",
                                           uncaught_exception_infos=uncaught_exception_infos)

        @final
        def c_join(self,
                   print_err: Union[bool, io.Pr.SupportsWrite[str]] = True) -> None:

            self.c_join_collect_reraise(runners=self.__f_runners,
                                        print_err=print_err)

    @dataclass(frozen=True)
    class FuncCallParams:
        func_or_obj_func: Union[Callable[..., Any], Tuple[Any, str]]
        """
        'func' is either a function to call or a tuple containing an object and a function name
        to retrieve as function from said object.
        """
        params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]]

    @dataclass(frozen=True)
    class ExecParams(FuncCallParams):
        join: bool
        print_err: Union[bool, io.Pr.SupportsWrite[str]] = True
        # print_err ignored if join is False
        runner_name: Optional[str] = None
        daemon: Optional[bool] = None

    @dataclass(frozen=True)
    class ExecDelayedParams(ExecParams):
        pre_delay: Optional[float] = None
        in_delay: Optional[float] = None
        post_delay: Optional[float] = None
        min_sec: Optional[float] = None
        max_sec: Optional[float] = None
