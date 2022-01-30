import multiprocessing.managers as mp_mngr
import time
from typing import Optional, Callable, Final, Any, Tuple

import pytest

from concurrency._fixtures import fix_csl, fix_manager, fix_picklable_manager,\
    fix_cslock_factory, fix_exec

from concurrency import cs, cslocks, execs
from utils.dynamic.inspection import CallableSignatureError
from utils.functional.tools import c_poll_condition, c_raises
from utils.testing.testasserts import de_assert_duration
import utils.types.containers as con


def __c_acquire(lock: cslocks.Pr.CSLock,
                blocking: bool,
                timeout: Optional[float],
                expect_success: bool,
                delay: Optional[float] = None) -> None:
    if delay is not None:
        time.sleep(delay)

    success: bool = lock.c_acquire(blocking=blocking,
                                   timeout=timeout)

    assert success == expect_success


def __c_acquire_context(lock: cslocks.Pr.CSLock,
                        expect_success: bool,
                        delay: Optional[float] = None) -> None:
    if delay is not None:
        time.sleep(delay)

    with lock as success:
        assert lock.c_is_held()
        assert success == expect_success


def __c_acquire_context_timeout(lock: cslocks.Pr.CSLock,
                                blocking: bool,
                                timeout: Optional[float],
                                expect_success: bool,
                                delay: Optional[float] = None) -> None:
    if delay is not None:
        time.sleep(delay)

    with lock.c_acquire_timeout(blocking=blocking,
                                timeout=timeout) as success:
        assert lock.c_is_held()
        assert success == expect_success


def __c_release(lock: cslocks.Pr.CSLock,
                delay: Optional[float] = None) -> None:
    if delay is not None:
        time.sleep(delay)

    lock.c_release()


def __c_re_release(lock: cslocks.Pr.CSLock) -> None:
    with pytest.raises(ValueError):
        lock.c_release()


def test_cs_lock(fix_csl: cs.En.CSL,
                 fix_manager: Optional[mp_mngr.SyncManager],
                 fix_cslock_factory: Callable[[cs.En.CSL,
                                               Optional[mp_mngr.SyncManager]],
                                              cslocks.Pr.CSLock],
                 fix_exec: execs.Im.Exec) -> None:
    cslock: Final[cslocks.Pr.CSLock] = fix_cslock_factory(fix_csl, fix_manager)

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

    assert not cslock.c_is_held()
    with pytest.raises(ValueError):
        cslock.c_release()

    _c_exec(csl=fix_csl,
            func=__c_re_release,
            params=((cslock,), {}),
            join=True)

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_acquire,
                                                                 params=((cslock, False, None, True, None), {}),
                                                                 join=True))()  # type: ignore
    assert cslock.c_is_held()
    de_assert_duration(min_sec=2.0, max_sec=3.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_acquire,
                                                                 params=((cslock, True, 2.0, False, None), {}),
                                                                 join=True))()  # type: ignore
    assert cslock.c_is_held()
    de_assert_duration(min_sec=2.0, max_sec=3.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_acquire_context_timeout,
                                                                 params=((cslock, True, 2.0, False, None), {}),
                                                                 join=True))()  # type: ignore
    assert cslock.c_is_held()
    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_release,
                                                                 params=((cslock, None), {}),
                                                                 join=True))()  # type: ignore

    assert not cslock.c_is_held()
    with pytest.raises(ValueError):
        cslock.c_release()
    _c_exec(csl=fix_csl,
            func=__c_re_release,
            params=((cslock,), {}),
            join=True)

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_acquire_context,
                                                                 params=((cslock, True, None), {}),
                                                                 join=True))()  # type: ignore
    assert not cslock.c_is_held()
    with pytest.raises(ValueError):
        cslock.c_release()
    _c_exec(csl=fix_csl,
            func=__c_re_release,
            params=((cslock,), {}),
            join=True)
    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_acquire_context_timeout,
                                                                 params=((cslock, True, 5.0, True, None), {}),
                                                                 join=True))()  # type: ignore
    assert not cslock.c_is_held()
    with pytest.raises(ValueError):
        cslock.c_release()
    _c_exec(csl=fix_csl,
            func=__c_re_release,
            params=((cslock,), {}),
            join=True)

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                     func=__c_acquire,
                                                                     params=((cslock, False, None, True, None), {}),
                                                                     join=True))()  # type: ignore
        assert cslock.c_is_held()
        de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                     func=__c_release,
                                                                     params=((cslock, 4.0), {}),
                                                                     join=False))()  # type: ignore
        assert cslock.c_is_held()
        de_assert_duration(min_sec=3.0, max_sec=5.0)(lambda: _c_exec(csl=fix_csl,
                                                                     func=__c_acquire_context_timeout,
                                                                     params=((cslock, True, 5.0, True, None), {}),
                                                                     join=True))()  # type: ignore
        assert not cslock.c_is_held()
        with pytest.raises(ValueError):
            cslock.c_release()
        _c_exec(csl=fix_csl,
                func=__c_re_release,
                params=((cslock,), {}),
                join=True)

    # Without joining, manager might get shutdown while
    # unjoined processes might still be running resulting in a Pipe error
    fix_exec.c_join()


_LOCK_1: Final[str] = "lock_1"
_LOCK_2: Final[str] = "lock_2"


# noinspection PyUnusedLocal
def test_cslockable_signature_error(fix_csl: cs.En.CSL,
                                    fix_manager: Optional[mp_mngr.SyncManager]) -> None:
    with pytest.raises(CallableSignatureError):
        class _CSLockable1(cslocks.Im.CSLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):

                super().__init__(csl=csl,
                                 cslock_keys=(lname for lname in (_LOCK_1, _LOCK_2)),
                                 manager=manager)

            @cslocks.De.d_cslock
            def foo(self,
                    arg1: int) -> int:
                return arg1

    with pytest.raises(CallableSignatureError):
        class _CSLockable2(cslocks.Im.CSLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):
                super().__init__(csl=csl,
                                 cslock_keys=(lname for lname in (_LOCK_1, _LOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @cslocks.De.d_cslock
            def foo(self,
                    arg1: int,
                    blocking: bool = True) -> int:
                return arg1

    with pytest.raises(CallableSignatureError):
        class _CSLockable3(cslocks.Im.CSLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):
                super().__init__(csl=csl,
                                 cslock_keys=(lname for lname in (_LOCK_1, _LOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @cslocks.De.d_cslock(_LOCK_1)
            def foo(self,
                    arg1: int,
                    blocking: bool = True,
                    timeout: Optional[float] = None) -> int:
                return arg1

    with pytest.raises(CallableSignatureError):
        class _CSLockable4(cslocks.Im.CSLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):
                super().__init__(csl=csl,
                                 cslock_keys=(lname for lname in (_LOCK_1, _LOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @cslocks.De.d_cslock(_LOCK_2)
            def foo(self,
                    arg1: int,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> int:
                return arg1

    class _CSLockable5(cslocks.Im.CSLockableMixin):

        def __init__(self,
                     csl: cs.En.CSL,
                     manager: Optional[mp_mngr.SyncManager]):
            super().__init__(csl=csl,
                             cslock_keys=(lname for lname in (_LOCK_1, _LOCK_2)),
                             manager=manager)

        # noinspection PyUnusedLocal
        @cslocks.De.d_cslock("asdffffffasd")
        def foo(self,
                arg1: int,
                blocking: bool = True,
                timeout: Optional[float] = None,
                _unsafe: bool = False) -> int:
            return arg1

    x: _CSLockable5 = _CSLockable5(csl=fix_csl,
                                   manager=fix_manager)
    with pytest.raises(cslocks.Er.UnknownLockName):
        # Unfortunately, this can only be checked when calling method (lock name to lock map gets initialized during
        # object creation.
        x.foo(22)

    with pytest.raises(KeyError):
        # Unfortunately, this can only be checked when calling method (lock name to lock map gets initialized during
        # object creation.
        x.foo(22)

    # _unsafe = True as default is forbidden
    with pytest.raises(CallableSignatureError):
        class _CSLockable6(cslocks.Im.CSLockableMixin):

            def __init__(self,
                         csl: cs.En.CSL,
                         manager: Optional[mp_mngr.SyncManager]):

                super().__init__(csl=csl,
                                 cslock_keys=(lname for lname in (_LOCK_1, _LOCK_2)),
                                 manager=manager)

            # noinspection PyUnusedLocal
            @cslocks.De.d_cslock
            def foo(self,
                    arg1: int,
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = True) -> int:
                return arg1

    class _CSLockable7(cslocks.Im.CSLockableMixin):

        def __init__(self,
                     csl: cs.En.CSL,
                     manager: Optional[mp_mngr.SyncManager]):
            super().__init__(csl=csl,
                             cslock_keys=(lname for lname in (_LOCK_1, _LOCK_2)),
                             manager=manager)

        # noinspection PyUnusedLocal
        @cslocks.De.d_cslock
        def foo(self,
                arg1: int,
                blocking: bool,
                timeout: Optional[float],
                _unsafe: bool) -> int:
            return arg1


# noinspection PyUnusedLocal
class _CSLockable(cslocks.Im.CSLockableMixin):

    def __init__(self,
                 csl: cs.En.CSL,
                 manager: Optional[mp_mngr.SyncManager]):

        super().__init__(csl=csl,
                         cslock_keys=(lname for lname in (_LOCK_1, _LOCK_2)),
                         manager=manager)

    @cslocks.De.d_cslock
    def foo_default_0(self,
                      arg1: int,
                      delay: float,
                      blocking: bool = True,
                      timeout: Optional[float] = None,
                      _unsafe: bool = False) -> int:
        time.sleep(delay)
        return arg1

    @cslocks.De.d_cslock
    def foo_default_1(self,
                      arg1: int,
                      delay: float,
                      blocking: bool = True,
                      timeout: Optional[float] = None,
                      _unsafe: bool = False) -> int:
        time.sleep(delay)
        return arg1

    @cslocks.De.d_cslock(_LOCK_1)
    def foo_lock1_0(self,
                    arg1: int,
                    delay: float,
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> int:
        time.sleep(delay)
        return arg1

    @cslocks.De.d_cslock(_LOCK_1)
    def foo_lock1_1(self,
                    arg1: int,
                    delay: float,
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> int:
        time.sleep(delay)
        return arg1

    @cslocks.De.d_cslock(_LOCK_2)
    def foo_lock2_0(self,
                    arg1: int,
                    delay: float,
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> int:
        time.sleep(delay)
        return arg1

    @cslocks.De.d_cslock(_LOCK_2)
    def foo_lock2_1(self,
                    arg1: int,
                    delay: float,
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> int:
        time.sleep(delay)
        return arg1


def __foo_default_0(x: _CSLockable,
                    arg1: int,
                    delay: float,
                    expect_timeout: bool,
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> None:

    if expect_timeout:
        with pytest.raises(TimeoutError):
            x.foo_default_0(arg1=arg1,
                            delay=delay,
                            blocking=blocking,
                            timeout=timeout,
                            _unsafe=_unsafe)
    else:
        x.foo_default_0(arg1=arg1,
                        delay=delay,
                        blocking=blocking,
                        timeout=timeout,
                        _unsafe=_unsafe)


def __foo_default_1(x: _CSLockable,
                    arg1: int,
                    delay: float,
                    expect_timeout: bool,
                    blocking: bool = True,
                    timeout: Optional[float] = None,
                    _unsafe: bool = False) -> None:

    if expect_timeout:
        with pytest.raises(TimeoutError):
            x.foo_default_1(arg1=arg1,
                            delay=delay,
                            blocking=blocking,
                            timeout=timeout,
                            _unsafe=_unsafe)
    else:
        x.foo_default_1(arg1=arg1,
                        delay=delay,
                        blocking=blocking,
                        timeout=timeout,
                        _unsafe=_unsafe)


def __foo_lock1_0(x: _CSLockable,
                  arg1: int,
                  delay: float,
                  expect_timeout: bool,
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> None:

    if expect_timeout:
        with pytest.raises(TimeoutError):
            x.foo_lock1_0(arg1=arg1,
                          delay=delay,
                          blocking=blocking,
                          timeout=timeout,
                          _unsafe=_unsafe)
    else:
        x.foo_lock1_0(arg1=arg1,
                      delay=delay,
                      blocking=blocking,
                      timeout=timeout,
                      _unsafe=_unsafe)


def __foo_lock1_1(x: _CSLockable,
                  arg1: int,
                  delay: float,
                  expect_timeout: bool,
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> None:

    if expect_timeout:
        with pytest.raises(TimeoutError):
            x.foo_lock1_1(arg1=arg1,
                          delay=delay,
                          blocking=blocking,
                          timeout=timeout,
                          _unsafe=_unsafe)
    else:
        x.foo_lock1_1(arg1=arg1,
                      delay=delay,
                      blocking=blocking,
                      timeout=timeout,
                      _unsafe=_unsafe)


def __foo_lock2_0(x: _CSLockable,
                  arg1: int,
                  delay: float,
                  expect_timeout: bool,
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> None:

    if expect_timeout:
        with pytest.raises(TimeoutError):
            x.foo_lock2_0(arg1=arg1,
                          delay=delay,
                          blocking=blocking,
                          timeout=timeout,
                          _unsafe=_unsafe)
    else:
        x.foo_lock2_0(arg1=arg1,
                      delay=delay,
                      blocking=blocking,
                      timeout=timeout,
                      _unsafe=_unsafe)


def __foo_lock2_1(x: _CSLockable,
                  arg1: int,
                  delay: float,
                  expect_timeout: bool,
                  blocking: bool = True,
                  timeout: Optional[float] = None,
                  _unsafe: bool = False) -> None:

    if expect_timeout:
        with pytest.raises(TimeoutError):
            x.foo_lock2_1(arg1=arg1,
                          delay=delay,
                          blocking=blocking,
                          timeout=timeout,
                          _unsafe=_unsafe)
    else:
        x.foo_lock2_1(arg1=arg1,
                      delay=delay,
                      blocking=blocking,
                      timeout=timeout,
                      _unsafe=_unsafe)


# noinspection PyUnusedLocal
def test_cslockable(fix_csl: cs.En.CSL,
                    fix_exec: execs.Im.Exec,
                    fix_manager: Optional[mp_mngr.SyncManager]) -> None:
    x: Final[_CSLockable] = _CSLockable(csl=fix_csl,
                                        manager=fix_manager)

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

    for foo_triple in ((__foo_default_0, __foo_default_1, __foo_lock1_0),
                       (__foo_lock1_0, __foo_lock1_1, __foo_default_0),
                       (__foo_lock2_0, __foo_lock2_1, __foo_lock1_0)):
        for foo in foo_triple:
            de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                         func=foo,
                                                                         params=((), {"x": x,
                                                                                      "arg1": 49,
                                                                                      "delay": 0.0,
                                                                                      "expect_timeout": False,
                                                                                      "blocking": True,
                                                                                      "timeout": None,
                                                                                      "_unsafe": False}),
                                                                         join=True))()  # type: ignore

        if fix_csl > cs.En.CSL.SINGLE_THREAD:
            de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                         func=foo_triple[0],
                                                                         params=((), {"x": x,
                                                                                      "arg1": 49,
                                                                                      "delay": 8.0,
                                                                                      "expect_timeout": False,
                                                                                      "blocking": True,
                                                                                      "timeout": None,
                                                                                      "_unsafe": False}),
                                                                         join=False))()  # type: ignore

            de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: c_poll_condition(
                condition_check=c_raises,
                params=((), {"raiser": foo_triple[0],
                             "params": ((), {"x": x,
                                             "arg1": 49,
                                             "delay": 0.0,
                                             "expect_timeout": False,
                                             "blocking": False,
                                             "timeout": None,
                                             "_unsafe": False}),
                             "expected_exception": TimeoutError}),
                max_duration=1.0))()  # type: ignore

            de_assert_duration(min_sec=1.0, max_sec=3.0)(lambda: _c_exec(csl=fix_csl,
                                                                         func=foo_triple[1],
                                                                         params=((), {"x": x,
                                                                                      "arg1": 49,
                                                                                      "delay": 0.0,
                                                                                      "expect_timeout": True,
                                                                                      "blocking": True,
                                                                                      "timeout": 2.0,
                                                                                      "_unsafe": False}),
                                                                         join=True))()  # type: ignore
            de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                         func=foo_triple[1],
                                                                         params=((), {"x": x,
                                                                                      "arg1": 49,
                                                                                      "delay": 0.0,
                                                                                      "expect_timeout": False,
                                                                                      "blocking": False,
                                                                                      "timeout": None,
                                                                                      "_unsafe": True}),
                                                                         join=True))()  # type: ignore
            de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                         func=foo_triple[2],
                                                                         params=((), {"x": x,
                                                                                      "arg1": 49,
                                                                                      "delay": 0.0,
                                                                                      "expect_timeout": False,
                                                                                      "blocking": False,
                                                                                      "timeout": None,
                                                                                      "_unsafe": False}),
                                                                         join=False))()  # type: ignore
            de_assert_duration(min_sec=4.0, max_sec=7.0)(lambda: _c_exec(csl=fix_csl,
                                                                         func=foo_triple[1],
                                                                         params=((), {"x": x,
                                                                                      "arg1": 49,
                                                                                      "delay": 0.0,
                                                                                      "expect_timeout": False,
                                                                                      "blocking": True,
                                                                                      "timeout": 7.0,
                                                                                      "_unsafe": False}),
                                                                         join=True))()  # type: ignore

    # Without joining, manager might get shutdown while
    # unjoined processes might still be running resulting in a Pipe error
    fix_exec.c_join()
