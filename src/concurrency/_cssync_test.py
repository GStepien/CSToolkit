from typing import Optional, Callable
import multiprocessing.managers as mp_mngr

import pytest

from concurrency._fixtures import fix_cscondition_factory, fix_csl, fix_manager, fix_picklable_manager,\
    fix_cslock_factory, fix_csrwlock_factory, fix_exec, fix_picklable_manager

from concurrency import cs, cslocks, cssync, execs, csrwlocks
import utils.functional.tools as ft
from utils.testing.testasserts import de_assert_duration
from concurrency import managers as mngr


def test_cscondition_parameters(fix_csl: cs.En.CSL,
                                fix_manager: Optional[mp_mngr.SyncManager],
                                fix_picklable_manager: Optional[mngr.Im.PicklableSyncManager],
                                fix_csrwlock_factory: Callable[[cs.En.CSL,
                                                                Optional[mp_mngr.SyncManager]],
                                                               csrwlocks.Pr.CSRWLock],
                                fix_cslock_factory: Callable[[cs.En.CSL,
                                                              Optional[mp_mngr.SyncManager]],
                                                             cslocks.Pr.CSLock],
                                fix_cscondition_factory: Callable[[cs.En.CSL,
                                                                   Optional[cslocks.Pr.CSLock],
                                                                   Optional[mngr.Im.PicklableSyncManager]],
                                                                  cssync.Pr.CSCondition],
                                fix_exec: execs.Im.Exec) -> None:

    with pytest.raises(ValueError):
        fix_cscondition_factory(fix_csl,
                                fix_csrwlock_factory(fix_csl,
                                                     fix_manager).c_get_csrlock(),
                                fix_picklable_manager)

    for lck in (None, fix_cslock_factory(fix_csl, fix_manager)):
        cscondition: cssync.Pr.CSCondition = fix_cscondition_factory(fix_csl,
                                                                     lck,
                                                                     fix_picklable_manager)

        ressource_lock: cslocks.Pr.CSLock = cscondition.c_get_resource_cslock()
        if lck is not None:
            assert lck is ressource_lock

        # Calling without holding resource lock
        with pytest.raises(ValueError):
            cscondition.c_wait(blocking=False)

        if fix_csl == cs.En.CSL.SINGLE_THREAD:
            with ressource_lock:
                with pytest.raises(RuntimeError):
                    cscondition.c_wait(blocking=True,
                                       timeout=None)
        else:
            # Calling without holding resource lock
            with pytest.raises(ValueError):
                cscondition.c_wait()

        # Calling without holding resource lock
        with pytest.raises(ValueError):
            cscondition.c_notify()
        # Calling without holding resource lock
        with pytest.raises(ValueError):
            cscondition.c_notify_all()
        # Calling without holding resource lock
        with pytest.raises(ValueError):
            cscondition.c_wait()

        with ressource_lock:
            cscondition.c_notify()
            cscondition.c_notify_all()

            assert not cscondition.c_wait(blocking=True,
                                          timeout=2.0)

            de_assert_duration(min_sec=2.0, max_sec=3.0)(  # type: ignore
                lambda: ft.c_poll_condition(condition_check=ft.c_eq,
                                            params=((), {
                                                'el_1': (cscondition,
                                                         cscondition.c_wait.__name__),
                                                'params_1': (None, {'blocking': True, 'timeout': 2.0}),
                                                'el_2': ft.c_identity,
                                                'params_2': ((False,), None)}),
                                            max_check_count=1))()

            assert ressource_lock.c_is_held()
            assert cscondition.c_get_num_waiting() == 0

            if fix_csl > cs.En.CSL.SINGLE_THREAD:

                # noinspection PyArgumentList
                fix_exec.c_exec_multiple(csl=fix_csl,
                                         join=False,
                                         exec_params=(
                                             # Waiter 1
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(cscondition,
                                                                   cscondition.c_wait.__name__),
                                                 params=(None, {'blocking': True,
                                                                'timeout': 6.0}),
                                                 join=False
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=ft.c_poll_condition,
                                                 params=(None, {
                                                     "condition_check": ft.c_eq,
                                                     "params": (None, {
                                                         "el_1": (cscondition,
                                                                  cscondition.c_get_num_waiting.__name__),
                                                         "el_2": ft.c_identity,
                                                         "params_2": (None, {
                                                             "el": 1
                                                         })
                                                     }),
                                                     "max_duration": 1.0
                                                 }),
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(ressource_lock,
                                                                   ressource_lock.c_acquire.__name__),
                                                 params=None,
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             # Waiter 2
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(cscondition,
                                                                   cscondition.c_wait.__name__),
                                                 params=(None, {'blocking': True,
                                                                'timeout': 6.0}),
                                                 join=False
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=ft.c_poll_condition,
                                                 params=(None, {
                                                     "condition_check": ft.c_eq,
                                                     "params": (None, {
                                                         "el_1": (cscondition,
                                                                  cscondition.c_get_num_waiting.__name__),
                                                         "el_2": ft.c_identity,
                                                         "params_2": (None, {
                                                             "el": 2
                                                         })
                                                     }),
                                                     "max_duration": 1.0
                                                 }),
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(ressource_lock,
                                                                   ressource_lock.c_acquire.__name__),
                                                 params=None,
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             # Waiter 3
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(cscondition,
                                                                   cscondition.c_wait.__name__),
                                                 params=(None, {'blocking': True,
                                                                'timeout': 6.0}),
                                                 join=False
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=ft.c_poll_condition,
                                                 params=(None, {
                                                     "condition_check": ft.c_eq,
                                                     "params": (None, {
                                                         "el_1": (cscondition,
                                                                  cscondition.c_get_num_waiting.__name__),
                                                         "el_2": ft.c_identity,
                                                         "params_2": (None, {
                                                             "el": 3
                                                         })
                                                     }),
                                                     "max_duration": 1.0
                                                 }),
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(ressource_lock,
                                                                   ressource_lock.c_acquire.__name__),
                                                 params=None,
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             # Waiter 4
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(cscondition,
                                                                   cscondition.c_wait.__name__),
                                                 params=(None, {'blocking': True,
                                                                'timeout': 6.0}),
                                                 join=False
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=ft.c_poll_condition,
                                                 params=(None, {
                                                     "condition_check": ft.c_eq,
                                                     "params": (None, {
                                                         "el_1": (cscondition,
                                                                  cscondition.c_get_num_waiting.__name__),
                                                         "el_2": ft.c_identity,
                                                         "params_2": (None, {
                                                             "el": 4
                                                         })
                                                     }),
                                                     "max_duration": 1.0
                                                 }),
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(ressource_lock,
                                                                   ressource_lock.c_acquire.__name__),
                                                 params=None,
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             # Notify n=2
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(cscondition,
                                                                   cscondition.c_notify.__name__),
                                                 params=(None, {"n": 2}),
                                                 join=True,
                                                 in_delay=2.0,
                                                 min_sec=2.0,
                                                 max_sec=3.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=ft.c_poll_condition,
                                                 params=(None, {
                                                     "condition_check": ft.c_eq,
                                                     "params": (None, {
                                                         "el_1": (cscondition,
                                                                  cscondition.c_get_num_waiting.__name__),
                                                         "el_2": ft.c_identity,
                                                         "params_2": (None, {
                                                             "el": 2
                                                         })
                                                     }),
                                                     "max_check_count": 1
                                                 }),
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(ressource_lock,
                                                                   ressource_lock.c_release.__name__),
                                                 params=None,
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=ft.c_poll_condition,
                                                 params=(None, {
                                                     "condition_check": (ressource_lock,
                                                                         ressource_lock.c_is_held.__name__),
                                                     "params": None,
                                                     "max_duration": 1.0
                                                 }),
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(ressource_lock,
                                                                   ressource_lock.c_release.__name__),
                                                 params=None,
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=ft.c_poll_condition,
                                                 params=(None, {
                                                     "condition_check": (ressource_lock,
                                                                         ressource_lock.c_is_held.__name__),
                                                     "params": None,
                                                     "max_duration": 1.0
                                                 }),
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             # Notify rest
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(cscondition,
                                                                   cscondition.c_notify_all.__name__),
                                                 params=None,
                                                 join=True,
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=ft.c_poll_condition,
                                                 params=(None, {
                                                     "condition_check": ft.c_eq,
                                                     "params": (None, {
                                                         "el_1": (cscondition,
                                                                  cscondition.c_get_num_waiting.__name__),
                                                         "el_2": ft.c_identity,
                                                         "params_2": (None, {
                                                             "el": 0
                                                         })
                                                     }),
                                                     "max_check_count": 1
                                                 }),
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(ressource_lock,
                                                                   ressource_lock.c_release.__name__),
                                                 params=None,
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=ft.c_poll_condition,
                                                 params=(None, {
                                                     "condition_check": (ressource_lock,
                                                                         ressource_lock.c_is_held.__name__),
                                                     "params": None,
                                                     "max_duration": 1.0
                                                 }),
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=(ressource_lock,
                                                                   ressource_lock.c_release.__name__),
                                                 params=None,
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                             execs.Im.ExecDelayedParams(
                                                 func_or_obj_func=ft.c_poll_condition,
                                                 params=(None, {
                                                     "condition_check": (ressource_lock,
                                                                         ressource_lock.c_is_held.__name__),
                                                     "params": None,
                                                     "max_duration": 1.0
                                                 }),
                                                 join=True,
                                                 min_sec=0.0,
                                                 max_sec=1.0
                                             ),
                                         ),
                                         manager=fix_manager,
                                         _min_sec=2.0,
                                         _max_sec=8.0),

        fix_exec.c_join()
