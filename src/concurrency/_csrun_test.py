import multiprocessing.managers as mp_mngr
import time
from typing import Optional, Final, Type, final, Union, Tuple

from concurrency._fixtures import fix_manager, fix_picklable_manager, \
    fix_csl, fix_csrunnable_manager, fix_exec

from concurrency import csrun, cslocks, cs, execs
import utils.functional.tools as ft
from utils.testing.testasserts import de_assert_duration
import utils.types.containers as con


class _CSRunnableException(Exception):
    pass


class _CSRunnable(csrun.Ab.CSRunnableMixin):

    __f_raise: Final[Optional[Type[BaseException]]]
    __f_pause: Optional[float]

    def __init__(self,
                 csl: cs.En.CSL,
                 raise_: Optional[Type[BaseException]] = None,
                 pause: Optional[float] = None,
                 manager: Optional[mp_mngr.SyncManager] = None):

        self.__f_pause = (pause
                          if pause is None
                          else max(0.0, pause))
        self.__f_raise = raise_

        super().__init__(csl=csl,
                         manager=manager)

    def _c_run(self) -> None:
        if self.__f_pause is not None:
            time.sleep(self.__f_pause)

        if self.__f_raise is not None:
            raise self.__f_raise


def __c_terminate_multiple(runnables: con.Pr.Iterable[csrun.Pr.CSRunnableMixin]) -> None:
    for runnable in runnables:
        runnable.c_terminate()


def __c_all_have_status(runnables: con.Pr.Iterable[csrun.Pr.CSRunnableMixin],
                        status: csrun.En.RunnerStatus) -> bool:
    return all(runnable.c_get_status() == status
               for runnable in runnables)


class _CSLoopRunnable(csrun.Ab.CSLoopRunnableMixin):

    __f_pre_loop_pause: Final[Optional[float]]
    __f_post_loop_pause: Final[Optional[float]]
    __f_raise: Final[Optional[Type[BaseException]]]

    def __init__(self,
                 csl: cs.En.CSL,
                 raise_: Optional[Type[BaseException]] = None,
                 pre_loop_pause: Optional[float] = None,
                 min_loop_duration: Optional[float] = cslocks.Co.f_POLLING_TIMEOUT,
                 post_loop_pause: Optional[float] = None,
                 manager: Optional[mp_mngr.SyncManager] = None):
        self.__f_pre_loop_pause = (pre_loop_pause
                                   if pre_loop_pause is None
                                   else max(0.0, pre_loop_pause))
        self.__f_post_loop_pause = (post_loop_pause
                                    if post_loop_pause is None
                                    else max(0.0, post_loop_pause))
        self.__f_raise = raise_

        super().__init__(csl=csl,
                         min_loop_duration=min_loop_duration,
                         manager=manager)

    @final
    def _c_init_loop(self) -> None:
        if self.__f_pre_loop_pause is not None:
            time.sleep(self.__f_pre_loop_pause)

    @final
    def _c_cleanup_loop(self) -> None:
        if self.__f_post_loop_pause is not None:
            time.sleep(self.__f_post_loop_pause)

    @final
    def _c_run_loop_iter(self) -> None:
        if self.__f_raise is not None:
            raise self.__f_raise


# noinspection PyArgumentList
def test_csrun(fix_csl: cs.En.CSL,
               fix_exec: execs.Im.Exec,
               fix_manager: Optional[mp_mngr.SyncManager],
               fix_csrunnable_manager: csrun.Pr.CSRunnableManager) -> None:

    assert len(fix_csrunnable_manager.c_get_csrunnables()) == 0

    csrunnable: Union[_CSRunnable, _CSLoopRunnable]

    # test exception - joining
    csrunnable = _CSRunnable(csl=fix_csl,
                             raise_=_CSRunnableException,
                             pause=4.0,
                             manager=fix_manager)
    try:
        fix_csrunnable_manager.c_exec(
            csl=fix_csl,
            exec_param=csrun.Im.CSRunnableExecParams(
                csrunnable=csrunnable,
                join=True,
                print_err=False),
            manager=fix_manager,
            _min_sec=4.0,
            _max_sec=5.0)
    except execs.Er.UncaughtException as e:
        assert len(e.UNCAUGHT_EXCEPTION_INFOS) == 1
        assert isinstance(e.UNCAUGHT_EXCEPTION_INFOS[0].exception, _CSRunnableException)
    else:
        assert False

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        # test exception - loop
        csrunnable = _CSRunnable(csl=fix_csl,
                                 raise_=_CSRunnableException,
                                 pause=4.0,
                                 manager=fix_manager)

        fix_csrunnable_manager.c_exec(
            csl=fix_csl,
            exec_param=csrun.Im.CSRunnableExecParams(
                csrunnable=csrunnable,
                join=False),
            manager=fix_manager)

        csrunnable = _CSLoopRunnable(csl=fix_csl,
                                     raise_=_CSRunnableException,
                                     pre_loop_pause=6.0,
                                     post_loop_pause=4.0,
                                     manager=fix_manager)

        fix_csrunnable_manager.c_exec(
            csl=fix_csl,
            exec_param=csrun.Im.CSRunnableExecParams(
                csrunnable=csrunnable,
                join=False),
            manager=fix_manager)

        try:
            de_assert_duration(min_sec=4.0, max_sec=7.0)(
                lambda: fix_csrunnable_manager.c_join(print_err=False))()  # type: ignore
        except execs.Er.UncaughtException as e:
            assert len(e.UNCAUGHT_EXCEPTION_INFOS) == 2
            assert isinstance(e.UNCAUGHT_EXCEPTION_INFOS[0].exception, _CSRunnableException)
            assert isinstance(e.UNCAUGHT_EXCEPTION_INFOS[1].exception, _CSRunnableException)
        else:
            assert False

        # test loop - terminate
        csrunnable = _CSLoopRunnable(csl=fix_csl,
                                     raise_=None,
                                     pre_loop_pause=4.0,
                                     post_loop_pause=4.0,
                                     manager=fix_manager)

        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=(
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': (csrunnable,
                                            csrunnable.c_is_not_started.__name__),
                        'params': None,
                        'max_check_count': 1
                    }),
                    join=True
                ),
            ),
            manager=fix_manager)

        fix_csrunnable_manager.c_exec(
            csl=fix_csl,
            exec_param=csrun.Im.CSRunnableExecParams(
                csrunnable=csrunnable,
                join=False),
            manager=fix_manager)

        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=(
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': (csrunnable,
                                            csrunnable.c_is_running.__name__),
                        'params': None,
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csrunnable,
                                     csrunnable.c_terminate.__name__),
                            'params_1': None,
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': csrun.En.RunnerStatus.RUNNING
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=False,
                    in_delay=6.0
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': (csrunnable,
                                            csrunnable.c_is_terminated.__name__),
                        'params': None,
                        'max_duration': 11.0
                    }),
                    join=True,
                    min_sec=8.0,
                    max_sec=11.0
                )
            ),
            manager=fix_manager,
            _min_sec=8.0,
            _max_sec=11.0)

        csrunnables: Tuple[csrun.Pr.CSRunnableMixin, ...] = (
            _CSLoopRunnable(csl=fix_csl,
                            pre_loop_pause=2.0,
                            post_loop_pause=4.0,
                            manager=fix_manager),
            _CSLoopRunnable(csl=fix_csl,
                            pre_loop_pause=2.0,
                            post_loop_pause=5.0,
                            manager=fix_manager),
            _CSLoopRunnable(csl=fix_csl,
                            pre_loop_pause=4.0,
                            post_loop_pause=6.0,
                            manager=fix_manager),
            _CSLoopRunnable(csl=fix_csl,
                            pre_loop_pause=1.0,
                            post_loop_pause=2.0,
                            manager=fix_manager)
        )

        csrunnable_params: Tuple[csrun.Im.CSRunnableExecParams, ...] = tuple(
            csrun.Im.CSRunnableExecParams(
                csrunnable=csrunnables[i],
                join=False)
            for i in range(len(csrunnables))
        )

        fix_csrunnable_manager.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=csrunnable_params,
            manager=fix_manager
        )

        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=(
                execs.Im.ExecParams(
                    func_or_obj_func=__c_terminate_multiple,
                    params=((), {
                        'runnables': csrunnables
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': __c_all_have_status,
                        'params': ((), {
                            'runnables': csrunnables,
                            'status': csrun.En.RunnerStatus.TERMINATED
                        }),
                        'max_duration': 12.0
                    }),
                    join=True
                )
            ),
            manager=fix_manager,
            _min_sec=8.0,
            _max_sec=12.0
        )

    fix_csrunnable_manager.c_join()
    assert len(fix_csrunnable_manager.c_get_csrunnables()) == 0
    fix_exec.c_join()
    assert len(fix_exec.c_get_runners()) == 0
