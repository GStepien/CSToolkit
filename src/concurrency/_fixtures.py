from typing import Any, Optional, Callable, Tuple, cast, Final

import pytest
from _pytest.fixtures import FixtureRequest

import multiprocessing as mp
import multiprocessing.managers as mp_mngr

from concurrency import cs, cslocks, csdata, csrun, cssync, execs
import concurrency._raw_csdata as rcsd
from concurrency import csrwlocks
from concurrency import managers as mngr
import utils.types.containers as con
from utils.types.typevars import TYPE, V_TYPE, K_TYPE


@pytest.fixture(scope="session", params=[(1, 55, -11, 555, 55),
                                         (1.4, 55.1, -1.2, 2.5, 55.5, 1.5),
                                         (12, -5.5, 14, 2.5, 112, -55, 12),
                                         (12, "asdf", 22, 55.5, "asddd", 2.5)])
def fix_tuple_leq_five(request: FixtureRequest) -> Tuple[Any, ...]:
    result: Any = getattr(request, "param")
    assert isinstance(result, tuple)
    return result


@pytest.fixture(scope="session", params=[rcsd.Ca.c_to_cs_value])
def fix_value_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                            TYPE,
                                                            Optional[mp_mngr.SyncManager]],
                                                           rcsd.Pr.Value[TYPE]]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          TYPE,
                          Optional[mp_mngr.SyncManager]],
                         rcsd.Pr.Value[TYPE]], result)


@pytest.fixture(scope="session", params=[rcsd.Ca.c_to_cs_mra,
                                         rcsd.Ca.c_to_cs_mlseq,
                                         csdata.Im.CSMutableSequence
                                         ])
def fix_mra_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                          con.Pr.Iterable[TYPE],
                                                          Optional[mp_mngr.SyncManager]],
                                                         con.Pr.MutableRandomAccess[TYPE]]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          con.Pr.Iterable[TYPE],
                          Optional[mp_mngr.SyncManager]],
                         con.Pr.MutableRandomAccess[TYPE]], result)


@pytest.fixture(scope="session", params=[rcsd.Ca.c_to_cs_mlseq,
                                         csdata.Im.CSMutableLengthSequence])
def fix_mlseq_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                            con.Pr.Iterable[TYPE],
                                                            Optional[mp_mngr.SyncManager]],
                                                           con.Pr.MutableLengthSequence[TYPE]]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          con.Pr.Iterable[TYPE],
                          Optional[mp_mngr.SyncManager]],
                         con.Pr.MutableLengthSequence[TYPE]], result)


@pytest.fixture(scope="function", params=[{1: 3, 5: 33, 33: 1},
                                          {2: 2.5, 55: 3, 33: -4.224},
                                          {"asdf": 2.5, 55: 333, 1134: "ggh"},
                                          {"asdf": -1.5, 55: 333, 1134: "ggh", "aggga": "gasdwet"}])
def fix_mapping(request: FixtureRequest) -> con.Pr.Mapping[Any, Any]:
    result: Any = getattr(request, "param")
    assert isinstance(result, con.Pr.Mapping)
    return result


@pytest.fixture(scope="session", params=[rcsd.Ca.c_to_cs_mm])
def fix_mm_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                         con.Pr.Mapping[K_TYPE, V_TYPE],
                                                         Optional[mp_mngr.SyncManager]],
                                                        con.Pr.MutableMapping[K_TYPE, V_TYPE]]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          con.Pr.Mapping[K_TYPE, V_TYPE],
                          Optional[mp_mngr.SyncManager]],
                         con.Pr.MutableMapping[K_TYPE, V_TYPE]], result)


@pytest.fixture(scope="function", params=[(), [3, 5, -112], (2, "asdf", -5.77, 44)])
def fix_init_queue(request: FixtureRequest) -> con.Pr.SizedIterable[Any]:
    result: Any = getattr(request, "param")
    assert isinstance(result, con.Pr.SizedIterable)
    return result


@pytest.fixture(scope="session", params=[rcsd.Ca.c_to_cs_queue,
                                         csdata.Im.CSCapacityQueue])
def fix_queue_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                            con.Pr.SizedIterable[TYPE],
                                                            Optional[int],
                                                            Optional[mp_mngr.SyncManager]],
                                                           con.Pr.Queue[TYPE]]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          con.Pr.SizedIterable[TYPE],
                          Optional[int],
                          Optional[mp_mngr.SyncManager]],
                         con.Pr.Queue[TYPE]], result)


@pytest.fixture(scope="session", params=[csdata.Im.CSCapacityQueue])
def fix_cscapacityqueue_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                                      con.Pr.SizedIterable[TYPE],
                                                                      Optional[int],
                                                                      Optional[mp_mngr.SyncManager]],
                                                                     csdata.Pr.CSCapacityQueue[TYPE]]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          con.Pr.SizedIterable[TYPE],
                          Optional[int],
                          Optional[mp_mngr.SyncManager]],
                         csdata.Pr.CSCapacityQueue[TYPE]], result)


@pytest.fixture(scope="session", params=[csdata.Im.CSChunkCapacityQueue])
def fix_cschunkcapacityqueue_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                                           con.Pr.SizedIterable[TYPE],
                                                                           Optional[int],
                                                                           Optional[int],
                                                                           Optional[int],
                                                                           Optional[mp_mngr.SyncManager]],
                                                                          csdata.Pr.CSChunkCapacityQueue[TYPE]]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          con.Pr.SizedIterable[TYPE],
                          Optional[int],
                          Optional[int],
                          Optional[int],
                          Optional[mp_mngr.SyncManager]],
                         csdata.Pr.CSChunkCapacityQueue[TYPE]], result)


@pytest.fixture(scope="session", params=[2, "asdf", -5.77, [3, (4, 5), "asdff"], (3, [4, 5], "asdff")])
def fix_init_csdata(request: FixtureRequest) -> Any:
    result: Any = getattr(request, "param")
    return result


@pytest.fixture(scope="session", params=[csdata.Im.CSData])
def fix_csdata_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                             TYPE,
                                                             Optional[mp_mngr.SyncManager]],
                                                            csdata.Pr.CSData[TYPE]]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          TYPE,
                          Optional[mp_mngr.SyncManager]],
                         csdata.Pr.CSData[TYPE]], result)


@pytest.fixture(scope="session", params=[(2, 2, 2, 4.4, 4.5, 4.5, -22.4),
                                         ["asf", 44.4, 44.4, (3.4, 5)],
                                         (3.4, "asdf", [4, 5.4], 2.2, "asdf", 4.5)])
def fix_init_csmseqdata(request: FixtureRequest) -> con.Pr.Iterable[Any]:
    result: Any = getattr(request, "param")
    return cast(con.Pr.Iterable[Any], result)


@pytest.fixture(scope="session", params=[csdata.Im.CSMutableSequence,
                                         csdata.Im.CSMutableLengthSequence])
def fix_csmseq_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                             con.Pr.Iterable[TYPE],
                                                             Optional[mp_mngr.SyncManager]],
                                                            csdata.Pr.CSMutableSequence[TYPE]]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          con.Pr.Iterable[TYPE],
                          Optional[mp_mngr.SyncManager]],
                         csdata.Pr.CSMutableSequence[TYPE]], result)


@pytest.fixture(scope="session", params=[csdata.Im.CSMutableLengthSequence])
def fix_csmlseq_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                              con.Pr.Iterable[TYPE],
                                                              Optional[mp_mngr.SyncManager]],
                                                             csdata.Pr.CSMutableLengthSequence[TYPE]]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          con.Pr.Iterable[TYPE],
                          Optional[mp_mngr.SyncManager]],
                         csdata.Pr.CSMutableLengthSequence[TYPE]], result)


@pytest.fixture(scope="session", params=[cs.En.CSL.SINGLE_THREAD, cs.En.CSL.MULTI_THREAD, cs.En.CSL.MULTI_PROCESS])
def fix_csl(request: FixtureRequest) -> cs.En.CSL:
    result: Any = getattr(request, "param")
    assert isinstance(result, cs.En.CSL)
    return result


@pytest.fixture(scope="function")
def fix_picklable_manager(fix_csl: cs.En.CSL) -> con.Pr.Iterable[Optional[mngr.Im.PicklableSyncManager]]:
    if fix_csl <= cs.En.CSL.MULTI_THREAD:
        yield None
    else:
        result: Final[mngr.Im.PicklableSyncManager] = mngr.Im.PicklableSyncManager()
        result.start()
        yield result
        result.shutdown()


@pytest.fixture(scope="function", params=[{'use_picklable': True}, {'use_picklable': False}])
def fix_manager(fix_csl: cs.En.CSL,
                fix_picklable_manager: Optional[mngr.Im.PicklableSyncManager],
                request: FixtureRequest) -> con.Pr.Iterable[Optional[mp_mngr.SyncManager]]:

    use_picklable: Any = getattr(request, 'param')
    assert isinstance(use_picklable, con.Pr.Mapping) and len(use_picklable) == 1 and 'use_picklable' in use_picklable
    use_picklable = use_picklable['use_picklable']
    assert isinstance(use_picklable, bool)

    result: Optional[mp_mngr.SyncManager]
    if fix_csl <= cs.En.CSL.MULTI_THREAD:
        assert fix_picklable_manager is None
        result = None
    else:
        assert fix_picklable_manager is not None
        if use_picklable:
            result = fix_picklable_manager
        else:
            result = mp.Manager()

    yield result

    assert (result is None) == (fix_csl <= cs.En.CSL.MULTI_THREAD)
    if result is not None:
        result.shutdown()


@pytest.fixture(scope="function")
def fix_exec() -> execs.Im.Exec:
    return execs.Im.Exec()


@pytest.fixture(scope="function")
def fix_csrunnable_manager() -> csrun.Pr.CSRunnableManager:
    return csrun.Im.CSRunnableManager()


# @pytest.fixture(scope="session", params=[1, 5, 10])
# def fix_runnable_num(request: FixtureRequest) -> int:
#     result: Any = getattr(request, "param")
#     assert isinstance(result, int)
#     return result


@pytest.fixture(scope="session", params=[lambda csl, manager: cslocks.Im.CSLock(csl=csl,
                                                                                manager=manager),
                                         lambda csl, manager: csrwlocks.Im.CSRWLock(csl=csl,
                                                                                    manager=manager).c_get_cswlock()])
def fix_cslock_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                             Optional[mp_mngr.SyncManager]],
                                                            cslocks.Pr.CSLock]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          Optional[mp_mngr.SyncManager]],
                         cslocks.Pr.CSLock], result)


@pytest.fixture(scope="session",
                params=[lambda csl, resource_lock, manager: cssync.Im.CSCondition(
                    csl=csl,
                    resource_lock=resource_lock,
                    manager=manager)])
def fix_cscondition_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                                  Optional[cslocks.Pr.CSLock],
                                                                  Optional[mngr.Im.PicklableSyncManager]],
                                                                 cssync.Pr.CSCondition]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          Optional[cslocks.Pr.CSLock],
                          Optional[mngr.Im.PicklableSyncManager]],
                         cssync.Pr.CSCondition], result)


@pytest.fixture(scope="session", params=[lambda csl, manager: csrwlocks.Im.CSRWLock(csl=csl,
                                                                                    manager=manager)])
def fix_csrwlock_factory(request: FixtureRequest) -> Callable[[cs.En.CSL,
                                                               Optional[mp_mngr.SyncManager]],
                                                              csrwlocks.Pr.CSRWLock]:
    result: Any = getattr(request, "param")
    assert callable(result)
    return cast(Callable[[cs.En.CSL,
                          Optional[mp_mngr.SyncManager]],
                         csrwlocks.Pr.CSRWLock], result)
