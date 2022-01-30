import multiprocessing.managers as mp_mngr
import time
from typing import Optional, Final, Callable, Any, Tuple, Union

import pytest

from concurrency._fixtures import fix_csl, fix_manager, fix_picklable_manager,\
    fix_csrwlock_factory, fix_exec

from concurrency import cs, execs, cslocks
from concurrency import csrwlocks
from utils.dynamic.inspection import CallableSignatureError
from utils.functional.tools import c_poll_condition, c_raises, c_not
from utils.testing.testasserts import de_assert_duration
import utils.types.containers as con


def __c_check_lock_consistency(rwlock: csrwlocks.Pr.CSRWLock) -> None:
    """
    NOTE: Only call if lock state does not change concurrently!
    """
    num_held: Final[int] = rwlock.c_get_csrlock().c_get_reader_num()
    assert num_held >= 0
    assert rwlock.c_is_held() == rwlock.c_get_cswlock().c_is_held() == rwlock.c_get_csrlock().c_is_held()
    assert rwlock.c_is_held_reading() == rwlock.c_get_csrlock().c_is_held_reading()
    assert rwlock.c_is_held_writing() == rwlock.c_get_cswlock().c_is_held_writing()
    assert not rwlock.c_is_held() or rwlock.c_is_held_writing() ^ rwlock.c_is_held_reading()
    assert num_held == 0 or rwlock.c_is_held_reading()


def __c_check_post_r_acquire_consistency(rwlock: csrwlocks.Pr.CSRWLock,
                                         success: bool) -> None:
    """
    NOTE: Only call if lock state does not change concurrently!
    """
    __c_check_lock_consistency(rwlock=rwlock)
    assert rwlock.c_is_held()

    if not success:
        assert rwlock.c_get_csrlock().c_get_reader_num() == 0
        assert rwlock.c_is_held_writing()
    else:
        assert rwlock.c_get_csrlock().c_get_reader_num() > 0
        assert rwlock.c_is_held_reading()


def __c_check_post_w_acquire_consistency(rwlock: csrwlocks.Pr.CSRWLock,
                                         success: bool) -> None:
    """
    NOTE: Only call if lock state does not change concurrently!
    """
    __c_check_lock_consistency(rwlock=rwlock)

    assert rwlock.c_is_held()
    if success:
        assert rwlock.c_is_held_writing()


def __c_r_acquire(rwlock: csrwlocks.Pr.CSRWLock,
                  blocking: bool,
                  timeout: Optional[float],
                  expect_success: bool,
                  fixed_lock_state: bool = True) -> None:

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)

    success: bool = rwlock.c_get_csrlock().c_acquire(blocking=blocking,
                                                     timeout=timeout)

    if fixed_lock_state:
        __c_check_post_r_acquire_consistency(rwlock=rwlock,
                                             success=success)

    assert success == expect_success


def __c_w_acquire(rwlock: csrwlocks.Pr.CSRWLock,
                  blocking: bool,
                  timeout: Optional[float],
                  expect_success: bool,
                  fixed_lock_state: bool = True) -> None:

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)

    success: bool = rwlock.c_get_cswlock().c_acquire(blocking=blocking,
                                                     timeout=timeout)

    if fixed_lock_state:
        __c_check_post_w_acquire_consistency(rwlock=rwlock,
                                             success=success)

    assert success == expect_success


def __c_r_acquire_context(rwlock: csrwlocks.Pr.CSRWLock,
                          expect_success: bool,
                          fixed_lock_state: bool = True) -> None:

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)

    num_held_before: Final[int] = rwlock.c_get_csrlock().c_get_reader_num()

    with rwlock.c_get_csrlock() as success:
        if fixed_lock_state:
            __c_check_post_r_acquire_consistency(rwlock=rwlock,
                                                 success=success)

        assert success == expect_success

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)
        assert rwlock.c_get_csrlock().c_get_reader_num() == num_held_before


def __c_w_acquire_context(rwlock: csrwlocks.Pr.CSRWLock,
                          expect_success: bool,
                          fixed_lock_state: bool = True) -> None:

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)

    with rwlock.c_get_cswlock() as success:
        if fixed_lock_state:
            __c_check_post_w_acquire_consistency(rwlock=rwlock,
                                                 success=success)

        assert success == expect_success

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)


def __c_r_acquire_context_timeout(rwlock: csrwlocks.Pr.CSRWLock,
                                  blocking: bool,
                                  timeout: Optional[float],
                                  expect_success: bool,
                                  fixed_lock_state: bool = True) -> None:

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)

    num_held_before: Final[int] = rwlock.c_get_csrlock().c_get_reader_num()

    with rwlock.c_get_csrlock().c_acquire_timeout(blocking=blocking,
                                                  timeout=timeout) as success:
        if fixed_lock_state:
            __c_check_post_r_acquire_consistency(rwlock=rwlock,
                                                 success=success)

        assert success == expect_success

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)
        assert rwlock.c_get_csrlock().c_get_reader_num() == num_held_before


def __c_w_acquire_context_timeout(rwlock: csrwlocks.Pr.CSRWLock,
                                  blocking: bool,
                                  timeout: Optional[float],
                                  expect_success: bool,
                                  fixed_lock_state: bool = True) -> None:

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)

    with rwlock.c_get_cswlock().c_acquire_timeout(blocking=blocking,
                                                  timeout=timeout) as success:
        if fixed_lock_state:
            __c_check_post_w_acquire_consistency(rwlock=rwlock,
                                                 success=success)

        assert success == expect_success

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)


def __c_r_release(rwlock: csrwlocks.Pr.CSRWLock,
                  fixed_lock_state: bool = True) -> None:

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)

    num_held_before: Final[int] = rwlock.c_get_csrlock().c_get_reader_num()

    if fixed_lock_state:
        assert rwlock.c_get_csrlock().c_is_held() and num_held_before > 0

    rwlock.c_get_csrlock().c_release()

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)
        assert rwlock.c_get_csrlock().c_get_reader_num() == num_held_before - 1
        assert ((num_held_before > 1 and rwlock.c_get_csrlock().c_is_held())
                or (num_held_before == 1 and not rwlock.c_get_csrlock().c_is_held()))


def __c_w_release(rwlock: csrwlocks.Pr.CSRWLock,
                  fixed_lock_state: bool = True) -> None:

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)

    num_held_before: Final[int] = rwlock.c_get_csrlock().c_get_reader_num()

    if fixed_lock_state:
        assert rwlock.c_get_cswlock().c_is_held() and num_held_before == 0

    rwlock.c_get_cswlock().c_release()

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)
        assert not rwlock.c_get_cswlock().c_is_held()
        assert rwlock.c_get_csrlock().c_get_reader_num() == num_held_before == 0


def __c_w_re_release(rwlock: csrwlocks.Pr.CSRWLock,
                     fixed_lock_state: bool = True) -> None:

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)

    num_held_before: Final[int] = rwlock.c_get_csrlock().c_get_reader_num()

    if fixed_lock_state:
        assert not rwlock.c_get_cswlock().c_is_held() or num_held_before > 0

    with pytest.raises(ValueError):
        rwlock.c_get_cswlock().c_release()

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)
        assert not rwlock.c_get_cswlock().c_is_held() or num_held_before > 0
        assert rwlock.c_get_csrlock().c_get_reader_num() == num_held_before


def __c_r_re_release(rwlock: csrwlocks.Pr.CSRWLock,
                     fixed_lock_state: bool = True) -> None:

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)

    num_held_before: Final[int] = rwlock.c_get_csrlock().c_get_reader_num()

    if fixed_lock_state:
        assert num_held_before == 0

    with pytest.raises(ValueError):
        rwlock.c_get_csrlock().c_release()

    if fixed_lock_state:
        __c_check_lock_consistency(rwlock=rwlock)
        assert rwlock.c_get_csrlock().c_get_reader_num() == num_held_before == 0


def test_cs_wlock(fix_csl: cs.En.CSL,
                  fix_manager: Optional[mp_mngr.SyncManager],
                  fix_csrwlock_factory: Callable[[cs.En.CSL,
                                                  Optional[mp_mngr.SyncManager]],
                                                 csrwlocks.Pr.CSRWLock],
                  fix_exec: execs.Im.Exec) -> None:

    csrwlock: Final[csrwlocks.Pr.CSRWLock] = fix_csrwlock_factory(fix_csl, fix_manager)

    assert not csrwlock.c_get_cswlock().c_is_held()

    def _c_exec(csl: cs.En.CSL,
                func: Callable[..., Any],
                params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]],
                join: bool) -> None:
        fix_exec._c_exec(csl=csl,
                         func=func,
                         params=params,
                         join=join,
                         manager=fix_manager,
                         runner_name=None,
                         daemon=None)

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": True,
                                                                     "timeout": None,
                                                                     "expect_success": True
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_cswlock().c_is_held()
    de_assert_duration(min_sec=2.0, max_sec=3.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": True,
                                                                     "timeout": 2.0,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_cswlock().c_is_held()
    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": False,
                                                                     "timeout": None,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_cswlock().c_is_held()
    de_assert_duration(min_sec=2.0, max_sec=3.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": True,
                                                                     "timeout": 2.0,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_cswlock().c_is_held()
    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": False,
                                                                     "timeout": None,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_cswlock().c_is_held()
    de_assert_duration(min_sec=2.0, max_sec=3.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_acquire_context_timeout,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": True,
                                                                     "timeout": 2.0,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_cswlock().c_is_held()
    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_acquire_context_timeout,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": False,
                                                                     "timeout": None,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_cswlock().c_is_held()
    de_assert_duration(min_sec=2.0, max_sec=3.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_acquire_context_timeout,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": True,
                                                                     "timeout": 2.0,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_cswlock().c_is_held()
    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_acquire_context_timeout,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": False,
                                                                     "timeout": None,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_cswlock().c_is_held()
    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_re_release,
                                                                 params=((), {
                                                                     "rwlock": csrwlock
                                                                 }),
                                                                 join=True))()  # type: ignore

    assert csrwlock.c_get_cswlock().c_is_held()
    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_release,
                                                                 params=((), {
                                                                     "rwlock": csrwlock
                                                                 }),
                                                                 join=True))()  # type: ignore
    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_re_release,
                                                                 params=((), {
                                                                     "rwlock": csrwlock
                                                                 }),
                                                                 join=True))()  # type: ignore

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_acquire_context,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "expect_success": True
                                                                 }),
                                                                 join=True))()  # type: ignore

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_acquire_context,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "expect_success": True
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert len(fix_exec.c_get_runners()) == 0


def test_cs_rlock(fix_csl: cs.En.CSL,
                  fix_manager: Optional[mp_mngr.SyncManager],
                  fix_csrwlock_factory: Callable[[cs.En.CSL,
                                                  Optional[mp_mngr.SyncManager]],
                                                 csrwlocks.Pr.CSRWLock],
                  fix_exec: execs.Im.Exec) -> None:

    csrwlock: Final[csrwlocks.Pr.CSRWLock] = fix_csrwlock_factory(fix_csl, fix_manager)

    assert not csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 0

    def _c_exec(csl: cs.En.CSL,
                func: Callable[..., Any],
                params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]],
                join: bool) -> None:
        fix_exec._c_exec(csl=csl,
                         func=func,
                         params=params,
                         join=join,
                         manager=fix_manager,
                         runner_name=None,
                         daemon=None)

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": True,
                                                                     "timeout": None,
                                                                     "expect_success": True
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 1

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": True,
                                                                     "timeout": None,
                                                                     "expect_success": True
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 2

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_acquire_context_timeout,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": False,
                                                                     "timeout": None,
                                                                     "expect_success": True
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 2

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_acquire_context,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "expect_success": True
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 2

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": True,
                                                                     "timeout": None,
                                                                     "expect_success": True
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 3

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": False,
                                                                     "timeout": None,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 3

    de_assert_duration(min_sec=1.0, max_sec=2.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_acquire_context_timeout,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": True,
                                                                     "timeout": 1.0,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 3

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_release,
                                                                 params=((), {
                                                                     "rwlock": csrwlock
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 2

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_release,
                                                                 params=((), {
                                                                     "rwlock": csrwlock
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 1

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": False,
                                                                     "timeout": None,
                                                                     "expect_success": False
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 1

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_re_release,
                                                                 params=((), {
                                                                     "rwlock": csrwlock
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 1

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_release,
                                                                 params=((), {
                                                                     "rwlock": csrwlock
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert not csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 0

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_r_re_release,
                                                                 params=((), {
                                                                     "rwlock": csrwlock
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert not csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 0

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_acquire,
                                                                 params=((), {
                                                                     "rwlock": csrwlock,
                                                                     "blocking": False,
                                                                     "timeout": None,
                                                                     "expect_success": True
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert csrwlock.c_get_cswlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 0

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_w_release,
                                                                 params=((), {
                                                                     "rwlock": csrwlock
                                                                 }),
                                                                 join=True))()  # type: ignore
    assert not csrwlock.c_get_csrlock().c_is_held()
    assert csrwlock.c_get_csrlock().c_get_reader_num() == 0

    assert len(fix_exec.c_get_runners()) == 0


# noinspection PyArgumentList
def test_cs_rwlock_waiting(fix_csl: cs.En.CSL,
                           fix_manager: Optional[mp_mngr.SyncManager],
                           fix_csrwlock_factory: Callable[[cs.En.CSL,
                                                           Optional[mp_mngr.SyncManager]],
                                                          csrwlocks.Pr.CSRWLock],
                           fix_exec: execs.Im.Exec) -> None:
    if fix_csl == cs.En.CSL.SINGLE_THREAD:
        return

    csrwlock: Final[csrwlocks.Pr.CSRWLock] = fix_csrwlock_factory(fix_csl, fix_manager)
    __c_check_lock_consistency(rwlock=csrwlock)

    def c_exec_multiple(csl: cs.En.CSL,
                        exec_: execs.Im.Exec,
                        join: bool,
                        exec_params: con.Pr.Sequence[Union[execs.Im.ExecParams,
                                                           execs.Im.ExecDelayedParams]],
                        _min_sec: Optional[float] = None,
                        _max_sec: Optional[float] = None) -> None:
        execs.Ca.c_exec_multiple(csl=csl,
                                 exec_=exec_,
                                 join=join,
                                 exec_params=exec_params,
                                 manager=fix_manager,
                                 _min_sec=_min_sec,
                                 _max_sec=_max_sec)

    # noinspection PyPep8Naming
    ExecDelayedParams = execs.Im.ExecDelayedParams

    # The outer de_assert_duration comes from 'c_exec_delayed_multiple' not having been thoroughly tested
    # at this point -> additional consistency test
    de_assert_duration(min_sec=0.0, max_sec=2.0)(
        lambda: c_exec_multiple(csl=fix_csl,
                                exec_=fix_exec,
                                join=False,
                                exec_params=[ExecDelayedParams(func_or_obj_func=__c_r_acquire,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'blocking': True,
                                                                            'timeout': None,
                                                                            'expect_success': True,
                                                                            'fixed_lock_state': False}),
                                                               join=True,
                                                               min_sec=0.0,
                                                               max_sec=1.0),
                                             ExecDelayedParams(func_or_obj_func=__c_r_acquire,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'blocking': True,
                                                                            'timeout': None,
                                                                            'expect_success': True,
                                                                            'fixed_lock_state': False}),
                                                               join=True,
                                                               min_sec=0.0,
                                                               max_sec=1.0)],
                                _min_sec=0.0,
                                _max_sec=2.0))()  # type: ignore

    assert csrwlock.c_get_csrlock().c_get_reader_num() == 2
    assert csrwlock.c_is_held_reading()
    __c_check_lock_consistency(rwlock=csrwlock)

    de_assert_duration(min_sec=8.0, max_sec=10.0)(
        lambda: c_exec_multiple(csl=fix_csl,
                                exec_=fix_exec,
                                join=False,
                                exec_params=[ExecDelayedParams(func_or_obj_func=__c_r_release,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'fixed_lock_state': False}),
                                                               join=False,
                                                               in_delay=4.0,
                                                               min_sec=0.0,
                                                               max_sec=1.0),
                                             ExecDelayedParams(func_or_obj_func=__c_r_release,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'fixed_lock_state': False}),
                                                               join=False,
                                                               in_delay=8.0,
                                                               min_sec=0.0,
                                                               max_sec=1.0),
                                             ExecDelayedParams(func_or_obj_func=__c_w_acquire,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'blocking': True,
                                                                            'timeout': None,
                                                                            'expect_success': True,
                                                                            'fixed_lock_state': False}),
                                                               join=True,
                                                               min_sec=7.0,
                                                               max_sec=9.0),
                                             ExecDelayedParams(func_or_obj_func=__c_w_release,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'fixed_lock_state': False}),
                                                               join=True,
                                                               min_sec=0.0,
                                                               max_sec=1.0)
                                             ],
                                _min_sec=8.0,
                                _max_sec=10.0))()  # type: ignore

    assert not csrwlock.c_is_held() and csrwlock.c_get_reader_num() == 0
    __c_check_lock_consistency(rwlock=csrwlock)

    de_assert_duration(min_sec=2.0, max_sec=4.0)(
        lambda: c_exec_multiple(csl=fix_csl,
                                exec_=fix_exec,
                                join=False,
                                exec_params=[ExecDelayedParams(func_or_obj_func=__c_w_acquire,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'blocking': False,
                                                                            'timeout': None,
                                                                            'expect_success': True,
                                                                            'fixed_lock_state': False}),
                                                               join=True,
                                                               min_sec=0.0,
                                                               max_sec=1.0),
                                             ExecDelayedParams(func_or_obj_func=__c_r_acquire,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'blocking': True,
                                                                            'timeout': 2.0,
                                                                            'expect_success': False,
                                                                            'fixed_lock_state': False}),
                                                               join=True,
                                                               min_sec=2.0,
                                                               max_sec=3.0)],
                                _min_sec=2.0,
                                _max_sec=4.0))()  # type: ignore

    assert csrwlock.c_is_held()
    assert csrwlock.c_get_reader_num() == 0
    assert csrwlock.c_is_held_writing()
    __c_check_lock_consistency(rwlock=csrwlock)

    de_assert_duration(min_sec=6.0, max_sec=8.0)(
        lambda: c_exec_multiple(csl=fix_csl,
                                exec_=fix_exec,
                                join=False,
                                exec_params=[ExecDelayedParams(func_or_obj_func=__c_w_release,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'fixed_lock_state': False}),
                                                               join=False,
                                                               in_delay=6.0,
                                                               min_sec=0.0,
                                                               max_sec=1.0),
                                             ExecDelayedParams(func_or_obj_func=__c_r_acquire,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'blocking': True,
                                                                            'timeout': None,
                                                                            'expect_success': True,
                                                                            'fixed_lock_state': False}),
                                                               join=True,
                                                               min_sec=5.0,
                                                               max_sec=7.0),
                                             ExecDelayedParams(func_or_obj_func=__c_r_release,
                                                               params=((), {'rwlock': csrwlock,
                                                                            'fixed_lock_state': False}),
                                                               join=True,
                                                               min_sec=0.0,
                                                               max_sec=1.0)],
                                _min_sec=6.0,
                                _max_sec=8.0))()  # type: ignore

    assert not csrwlock.c_is_held()
    assert csrwlock.c_get_reader_num() == 0
    __c_check_lock_consistency(rwlock=csrwlock)

    # Without joining, manager might get shutdown while
    # unjoined processes might still be running resulting in a Pipe error
    fix_exec.c_join()


_RWLOCK_1: Final[str] = "rwlock_1"
_RWLOCK_2: Final[str] = "rwlock_2"


# noinspection PyUnusedLocal
def test_csrwlockable_signature_error(fix_csl: cs.En.CSL,
                                      fix_manager: Optional[mp_mngr.SyncManager]) -> None:

    with pytest.raises(CallableSignatureError):
        class _CSRWLockable1(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):

                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            @csrwlocks.De.d_csrlock
            def foo(self) -> None:
                pass

    with pytest.raises(CallableSignatureError):
        class _CSRWLockable2(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):

                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @csrwlocks.De.d_csrlock
            def foo(self,
                    arg1: int,
                    blocking: bool = True) -> int:
                return arg1

    with pytest.raises(CallableSignatureError):
        class _CSRWLockable3(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):

                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @csrwlocks.De.d_csrlock(_RWLOCK_1)
            def foo(self,
                    arg1: int,
                    blocking: bool = True,
                    timeout: Optional[float] = None) -> int:
                return arg1

    with pytest.raises(CallableSignatureError):
        class _CSLockable4(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):

                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @csrwlocks.De.d_csrlock(_RWLOCK_2)
            def foo(self,
                    arg1: int,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> int:
                return arg1

    class _CSLockable5(csrwlocks.Im.CSRWLockableMixin):

        def __init__(self,
                     csl: cs.En.CSL,
                     manager: Optional[mp_mngr.SyncManager]):
            super().__init__(csl=csl,
                             csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                             manager=manager)

        # noinspection PyUnusedLocal
        @csrwlocks.De.d_csrlock("asdffffffasd")
        def foo(self,
                arg1: int,
                blocking: bool = True,
                timeout: Optional[float] = None,
                _unsafe: bool = False) -> int:
            return arg1

    x5: Final[_CSLockable5] = _CSLockable5(csl=fix_csl,
                                           manager=fix_manager)
    with pytest.raises(cslocks.Er.UnknownLockName):
        # Unfortunately, this can only be checked when calling method (lock name to lock map gets initialized during
        # object creation.
        x5.foo(22)

    with pytest.raises(KeyError):
        # Unfortunately, this can only be checked when calling method (lock name to lock map gets initialized during
        # object creation.
        x5.foo(22)

    with pytest.raises(CallableSignatureError):
        class _CSRWLockable6(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):

                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            @csrwlocks.De.d_cswlock
            def foo(self) -> None:
                pass

    with pytest.raises(CallableSignatureError):
        class _CSRWLockable7(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):

                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @csrwlocks.De.d_cswlock
            def foo(self,
                    arg1: int,
                    blocking: bool = True) -> int:
                return arg1

    with pytest.raises(CallableSignatureError):
        class _CSRWLockable8(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):

                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @csrwlocks.De.d_cswlock(_RWLOCK_1)
            def foo(self,
                    arg1: int,
                    blocking: bool = True,
                    timeout: Optional[float] = None) -> int:
                return arg1

    with pytest.raises(CallableSignatureError):
        class _CSLockable9(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):

                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @csrwlocks.De.d_cswlock(_RWLOCK_2)
            def foo(self,
                    arg1: int,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> int:
                return arg1

    class _CSLockable10(csrwlocks.Im.CSRWLockableMixin):

        def __init__(self,
                     csl: cs.En.CSL,
                     manager: Optional[mp_mngr.SyncManager]):
            super().__init__(csl=csl,
                             csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                             manager=manager)

        # noinspection PyUnusedLocal
        @csrwlocks.De.d_cswlock("asdffffffasd")
        def foo(self,
                arg1: int,
                blocking: bool = True,
                timeout: Optional[float] = None,
                _unsafe: bool = False) -> int:
            return arg1

    x10: Final[_CSLockable10] = _CSLockable10(csl=fix_csl,
                                              manager=fix_manager)

    with pytest.raises(cslocks.Er.UnknownLockName):
        # Unfortunately, this can only be checked when calling method (lock name to lock map gets initialized during
        # object creation.
        x10.foo(22)

    with pytest.raises(KeyError):
        # Unfortunately, this can only be checked when calling method (lock name to lock map gets initialized during
        # object creation.
        x10.foo(22)

    # _unsafe = True as default is forbidden
    with pytest.raises(CallableSignatureError):
        class _CSRWLockable11(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):
                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @csrwlocks.De.d_csrlock
            def foo(self,
                    arg1: int,
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = True) -> int:
                return arg1

    # _unsafe = True as default is forbidden
    with pytest.raises(CallableSignatureError):
        class _CSRWLockable12(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):
                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @csrwlocks.De.d_cswlock
            def foo(self,
                    arg1: int,
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = True) -> int:
                return arg1

        class _CSRWLockable13(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):
                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @csrwlocks.De.d_csrlock
            def foo(self,
                    arg1: int,
                    blocking: bool,
                    timeout: Optional[float],
                    _unsafe: bool) -> int:
                return arg1

        class _CSRWLockable14(csrwlocks.Im.CSRWLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):
                super().__init__(csl=csl,
                                 csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @csrwlocks.De.d_cswlock
            def foo(self,
                    arg1: int,
                    blocking: bool,
                    timeout: Optional[float],
                    _unsafe: bool) -> int:
                return arg1


# noinspection PyUnusedLocal
class _CSRWLockable(csrwlocks.Im.CSRWLockableMixin):

    def __init__(self,
                 csl: cs.En.CSL,
                 manager: Optional[mp_mngr.SyncManager]):

        super().__init__(csl=csl,
                         csrwlock_keys=(lname for lname in (_RWLOCK_1, _RWLOCK_2)),
                         manager=manager)

    @csrwlocks.De.d_csrlock
    def foo_default_r(self,
                      delay: Optional[float],
                      blocking: bool = True,
                      timeout: Optional[float] = None,
                      _unsafe: bool = False) -> None:
        if delay is not None and delay > 0:
            time.sleep(delay)

    @csrwlocks.De.d_cswlock
    def foo_default_w(self,
                      delay: Optional[float],
                      blocking: bool = True,
                      timeout: Optional[float] = None,
                      _unsafe: bool = False) -> None:
        if delay is not None and delay > 0:
            time.sleep(delay)

    @csrwlocks.De.d_csrlock(_RWLOCK_1)
    def foo_lock_1_r(self,
                     delay: Optional[float],
                     blocking: bool = True,
                     timeout: Optional[float] = None,
                     _unsafe: bool = False) -> None:
        if delay is not None and delay > 0:
            time.sleep(delay)

    @csrwlocks.De.d_cswlock(_RWLOCK_1)
    def foo_lock_1_w(self,
                     delay: Optional[float],
                     blocking: bool = True,
                     timeout: Optional[float] = None,
                     _unsafe: bool = False) -> None:
        if delay is not None and delay > 0:
            time.sleep(delay)

    @csrwlocks.De.d_csrlock(_RWLOCK_2)
    def foo_lock_2_r(self,
                     delay: Optional[float],
                     blocking: bool = True,
                     timeout: Optional[float] = None,
                     _unsafe: bool = False) -> None:
        if delay is not None and delay > 0:
            time.sleep(delay)

    @csrwlocks.De.d_cswlock(_RWLOCK_2)
    def foo_lock_2_w(self,
                     delay: Optional[float],
                     blocking: bool = True,
                     timeout: Optional[float] = None,
                     _unsafe: bool = False) -> None:
        if delay is not None and delay > 0:
            time.sleep(delay)


# noinspection PyArgumentList
def test_cs_rwlockable_timeout(fix_csl: cs.En.CSL,
                               fix_manager: Optional[mp_mngr.SyncManager],
                               fix_exec: execs.Im.Exec) -> None:
    x: Final[_CSRWLockable] = _CSRWLockable(csl=fix_csl,
                                            manager=fix_manager)

    def c_exec_multiple(csl: cs.En.CSL,
                        exec_: execs.Im.Exec,
                        join: bool,
                        exec_params: con.Pr.Sequence[Union[execs.Im.ExecParams,
                                                           execs.Im.ExecDelayedParams]],
                        _min_sec: Optional[float] = None,
                        _max_sec: Optional[float] = None) -> None:
        execs.Ca.c_exec_multiple(csl=csl,
                                 exec_=exec_,
                                 join=join,
                                 exec_params=exec_params,
                                 manager=fix_manager,
                                 _min_sec=_min_sec,
                                 _max_sec=_max_sec)

    # noinspection PyPep8Naming
    ExecDelayedParams = execs.Im.ExecDelayedParams

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        for r_name, w_name, lock_key in ((x.foo_default_r.__name__, x.foo_default_w.__name__, csrwlocks.Co.f_DEFAULT_CSRWLOCK_KEY),
                                         (x.foo_lock_1_r.__name__, x.foo_lock_1_w.__name__, _RWLOCK_1),
                                         (x.foo_lock_2_r.__name__, x.foo_lock_2_w.__name__, _RWLOCK_2)):

            c_exec_multiple(
                csl=fix_csl,
                exec_=fix_exec,
                join=False,
                exec_params=[ExecDelayedParams(func_or_obj_func=(x,
                                                                 r_name),
                                               params=((), {'delay': 2.0,
                                                            'blocking': True,
                                                            'timeout': None}),
                                               join=False,
                                               min_sec=0.0,
                                               max_sec=1.0),
                             ExecDelayedParams(func_or_obj_func=(x,
                                                                 r_name),
                                               params=((), {'delay': 6.0,
                                                            'blocking': True,
                                                            'timeout': None}),
                                               join=False,
                                               min_sec=0.0,
                                               max_sec=1.0),
                             ExecDelayedParams(func_or_obj_func=(x,
                                                                 r_name),
                                               params=((), {'delay': 4.0,
                                                            'blocking': True,
                                                            'timeout': None}),
                                               join=False,
                                               min_sec=0.0,
                                               max_sec=1.0),
                             ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                               params=((), {'condition_check': (x.c_get_csrwlock(lock_key=lock_key),
                                                                                x.c_get_csrwlock(lock_key=lock_key).c_is_held.__name__),
                                                            'params': ((), {}),
                                                            'max_duration': 1.0}),
                                               join=True,
                                               min_sec=0.0,
                                               max_sec=1.0),
                             ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                               params=((), {'condition_check': c_raises,
                                                            'params': ((), {'raiser': (x,
                                                                                       w_name),
                                                                            'params': ((), {'delay': None,
                                                                                            'blocking': True,
                                                                                            'timeout': 2.0}),
                                                                            'expected_exception': TimeoutError}),
                                                            'max_duration': 0.0,
                                                            'max_check_count': 1}),
                                               join=True,
                                               min_sec=2.0,
                                               max_sec=3.0),
                             ExecDelayedParams(func_or_obj_func=(x,
                                                                 w_name),
                                               params=((), {'delay': None,
                                                            'blocking': True,
                                                            'timeout': None}),
                                               join=True,
                                               min_sec=2.0,
                                               max_sec=5.0)],
                _min_sec=6.0,
                _max_sec=8.0)

            c_exec_multiple(
                csl=fix_csl,
                exec_=fix_exec,
                join=False,
                exec_params=[ExecDelayedParams(func_or_obj_func=(x,
                                                                 w_name),
                                               params=((), {'delay': 8.0,
                                                            'blocking': True,
                                                            'timeout': None}),
                                               join=False,
                                               min_sec=0.0,
                                               max_sec=1.0),
                             ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                               params=((), {'condition_check': (x.c_get_csrwlock(lock_key=lock_key),
                                                                                x.c_get_csrwlock(lock_key=lock_key).c_is_held.__name__),
                                                            'params': ((), {}),
                                                            'max_duration': 1.0}),
                                               join=True,
                                               min_sec=0.0,
                                               max_sec=1.0),
                             ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                               params=((), {'condition_check': c_raises,
                                                            'params': ((), {'raiser': (x,
                                                                                       r_name),
                                                                            'params': ((), {'delay': None,
                                                                                            'blocking': True,
                                                                                            'timeout': 2.0}),
                                                                            'expected_exception': TimeoutError}),
                                                            'max_duration': 0.0,
                                                            'max_check_count': 1}),
                                               join=True,
                                               min_sec=2.0,
                                               max_sec=3.0),
                             ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                               params=((), {'condition_check': c_raises,
                                                            'params': ((), {'raiser': (x,
                                                                                       w_name),
                                                                            'params': ((), {'delay': None,
                                                                                            'blocking': True,
                                                                                            'timeout': 2.0}),
                                                                            'expected_exception': TimeoutError}),
                                                            'max_duration': 0.0,
                                                            'max_check_count': 1}),
                                               join=True,
                                               min_sec=2.0,
                                               max_sec=3.0),
                             ExecDelayedParams(func_or_obj_func=(x,
                                                                 r_name),
                                               params=((), {'delay': None,
                                                            'blocking': True,
                                                            'timeout': None}),
                                               join=True,
                                               min_sec=2.0,
                                               max_sec=5.0)],
                _min_sec=8.0,
                _max_sec=10.0)

    # Without joining, manager might get shutdown while
    # unjoined processes might still be running resulting in a Pipe error
    fix_exec.c_join()


# noinspection PyArgumentList
def test_cs_rwlockable(fix_csl: cs.En.CSL,
                       fix_manager: Optional[mp_mngr.SyncManager],
                       fix_exec: execs.Im.Exec) -> None:
    x: Final[_CSRWLockable] = _CSRWLockable(csl=fix_csl,
                                            manager=fix_manager)

    def c_exec_multiple(csl: cs.En.CSL,
                        exec_: execs.Im.Exec,
                        join: bool,
                        exec_params: con.Pr.Sequence[Union[execs.Im.ExecParams,
                                                           execs.Im.ExecDelayedParams]],
                        _min_sec: Optional[float] = None,
                        _max_sec: Optional[float] = None) -> None:
        execs.Ca.c_exec_multiple(csl=csl,
                                 exec_=exec_,
                                 join=join,
                                 exec_params=exec_params,
                                 manager=fix_manager,
                                 _min_sec=_min_sec,
                                 _max_sec=_max_sec)

    # noinspection PyPep8Naming
    ExecDelayedParams = execs.Im.ExecDelayedParams

    for r_name, w_name in ((x.foo_default_r.__name__, x.foo_default_w.__name__),
                           (x.foo_lock_1_r.__name__, x.foo_lock_1_w.__name__),
                           (x.foo_lock_2_r.__name__, x.foo_lock_2_w.__name__)):

        c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[ExecDelayedParams(func_or_obj_func=(x,
                                                             r_name),
                                           params=((), {'delay': 2.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=True,
                                           pre_delay=2.0,
                                           in_delay=2.0,
                                           post_delay=2.0,
                                           min_sec=8.0,
                                           max_sec=9.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             w_name),
                                           params=((), {'delay': None,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=True,
                                           min_sec=0.0,
                                           max_sec=1.0)],
            _min_sec=8.0,
            _max_sec=10.0)

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_default_r.__name__),
                                           params=((), {'delay': 2.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=False,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_default_r.__name__),
                                           params=((), {'delay': 4.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=False,
                                           min_sec=0.0,
                                           max_sec=1.0),

                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_1_r.__name__),
                                           params=((), {'delay': 6.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=False,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_1_r.__name__),
                                           params=((), {'delay': 8.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=False,
                                           min_sec=0.0,
                                           max_sec=1.0),

                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_2_r.__name__),
                                           params=((), {'delay': 10.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=False,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_2_r.__name__),
                                           params=((), {'delay': 12.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=False,
                                           min_sec=0.0,
                                           max_sec=1.0),

                         ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                           params=((), {'condition_check': (x.c_get_csrwlock(),
                                                                            x.c_get_csrwlock().c_is_held.__name__),
                                                        'params': ((), {}),
                                                        'max_duration': 1.0}),
                                           join=True,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                           params=((), {'condition_check': (x.c_get_csrwlock(lock_key=_RWLOCK_1),
                                                                            x.c_get_csrwlock(lock_key=_RWLOCK_1).c_is_held.__name__),
                                                        'params': ((), {}),
                                                        'max_duration': 1.0}),
                                           join=True,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                           params=((), {'condition_check': (x.c_get_csrwlock(lock_key=_RWLOCK_2),
                                                                            x.c_get_csrwlock(lock_key=_RWLOCK_2).c_is_held.__name__),
                                                        'params': ((), {}),
                                                        'max_duration': 1.0}),
                                           join=True,
                                           min_sec=0.0,
                                           max_sec=1.0),

                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_default_w.__name__),
                                           params=((), {'delay': None,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=True,
                                           # Must be a bit shorter as time has passed since we acquired r lock
                                           min_sec=2.0,
                                           max_sec=5.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_1_w.__name__),
                                           params=((), {'delay': None,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=True,
                                           min_sec=2.0,
                                           max_sec=5.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_2_w.__name__),
                                           params=((), {'delay': 2.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=True,
                                           min_sec=4.0,
                                           max_sec=7.0)
                         ],
            _min_sec=13.0,
            _max_sec=15.0)

        c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_default_w.__name__),
                                           params=((), {'delay': 4.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=False,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_1_w.__name__),
                                           params=((), {'delay': 8.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=False,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_2_w.__name__),
                                           params=((), {'delay': 12.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=False,
                                           min_sec=0.0,
                                           max_sec=1.0),

                         ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                           params=((), {'condition_check': (x.c_get_csrwlock(),
                                                                            x.c_get_csrwlock().c_is_held.__name__),
                                                        'params': ((), {}),
                                                        'max_duration': 1.0}),
                                           join=True,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                           params=(
                                               (), {'condition_check': (x.c_get_csrwlock(lock_key=_RWLOCK_1),
                                                                        x.c_get_csrwlock(
                                                                            lock_key=_RWLOCK_1).c_is_held.__name__),
                                                    'params': ((), {}),
                                                    'max_duration': 1.0}),
                                           join=True,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                           params=(
                                               (), {'condition_check': (x.c_get_csrwlock(lock_key=_RWLOCK_2),
                                                                        x.c_get_csrwlock(
                                                                            lock_key=_RWLOCK_2).c_is_held.__name__),
                                                    'params': ((), {}),
                                                    'max_duration': 1.0}),
                                           join=True,
                                           min_sec=0.0,
                                           max_sec=1.0),

                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_default_r.__name__),
                                           params=((), {'delay': None,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=True,
                                           # Must be a bit shorter as time has passed since we acquired w lock
                                           min_sec=2.0,
                                           max_sec=5.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_default_r.__name__),
                                           params=((), {'delay': None,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=True,
                                           min_sec=0.0,
                                           max_sec=1.0),

                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_1_r.__name__),
                                           params=((), {'delay': None,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=True,
                                           min_sec=2.0,
                                           max_sec=5.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_1_r.__name__),
                                           params=((), {'delay': None,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=True,
                                           min_sec=0.0,
                                           max_sec=1.0),

                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_2_r.__name__),
                                           params=((), {'delay': 2.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=True,
                                           min_sec=4.0,
                                           max_sec=7.0),
                         ExecDelayedParams(func_or_obj_func=(x,
                                                             x.foo_lock_2_r.__name__),
                                           params=((), {'delay': 4.0,
                                                        'blocking': True,
                                                        'timeout': None}),
                                           join=False,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                           params=(
                                               (), {'condition_check': (x.c_get_csrwlock(lock_key=_RWLOCK_2),
                                                                        x.c_get_csrwlock(
                                                                            lock_key=_RWLOCK_2).c_is_held.__name__),
                                                    'params': ((), {}),
                                                    'max_duration': 1.0}),
                                           join=True,
                                           min_sec=0.0,
                                           max_sec=1.0),
                         ExecDelayedParams(func_or_obj_func=c_poll_condition,
                                           params=(
                                               (), {'condition_check': c_not,
                                                    'params': ((), {'condition_check': (x.c_get_csrwlock(lock_key=_RWLOCK_2),
                                                                                        x.c_get_csrwlock(lock_key=_RWLOCK_2).c_is_held.__name__)}),
                                                    'max_duration': 5.0}),
                                           join=True,
                                           min_sec=2.0,
                                           max_sec=5.0)
                         ],
            _min_sec=16.0,
            _max_sec=20.0)

    # Without joining, manager might get shutdown while
    # unjoined processes might still be running resulting in a Pipe error
    fix_exec.c_join()
