import time
from multiprocessing.managers import ListProxy  # type: ignore
from queue import Full, Empty
from typing import Final, Optional, Union, Tuple, Any, Callable, List, Dict

import multiprocessing.managers as mp_mngr

import pytest

from concurrency._fixtures import fix_csl, fix_manager, fix_picklable_manager, \
    fix_tuple_leq_five, fix_mra_factory, fix_mlseq_factory, fix_mm_factory, fix_mapping, \
    fix_init_queue, fix_queue_factory, fix_value_factory, fix_exec

from concurrency import cs, execs
import concurrency._raw_csdata as rcsd
from utils.testing.testasserts import de_assert_duration, TooLongDuration, TooShortDuration
import utils.types.containers as con
from utils.types.typevars import TYPE, K_TYPE, V_TYPE


def __c_value_add_int(val_: rcsd.Pr.Value[Union[int, float]],
                      add: int) -> None:
    val_.value = val_.value + add


def __c_value_mult_int(val_: rcsd.Pr.Value[Union[int, float]],
                       factor: int) -> None:
    val_.value = val_.value * factor


def __c_value_inv(val_: rcsd.Pr.Value[bool]) -> None:
    val_.value = not val_.value


def __c_value_set_str(val_: rcsd.Pr.Value[str],
                      string: str) -> None:
    val_.value = string


def test_cs_num_value(fix_csl: cs.En.CSL,
                      fix_manager: Optional[mp_mngr.SyncManager],
                      fix_value_factory: Callable[[cs.En.CSL,
                                                   TYPE,
                                                   Optional[mp_mngr.SyncManager]],
                                                  rcsd.Pr.Value[TYPE]],
                      fix_exec: execs.Im.Exec) -> None:

    init_vals: Final[Tuple[Any, ...]] = (-2., 1.5, 55, -42.55, 2.5, 122)

    vals: Final[Tuple[rcsd.Pr.Value[Any], ...]] = \
        tuple(fix_value_factory(fix_csl, init_val, fix_manager)
              for init_val in init_vals)

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

    for idx in range(len(init_vals)):

        _c_exec(csl=fix_csl,
                func=__c_value_add_int,
                params=((vals[idx], 5), {}),
                join=True)

        assert vals[idx].value == (init_vals[idx] + 5)

        _c_exec(csl=fix_csl,
                func=__c_value_mult_int,
                params=(tuple(), {"val_": vals[idx], "factor": 2}),
                join=True)

        assert vals[idx].value == (init_vals[idx] + 5) * 2

    assert len(fix_exec.c_get_runners()) == 0


def test_cs_bool_value(fix_csl: cs.En.CSL,
                       fix_manager: Optional[mp_mngr.SyncManager],
                       fix_value_factory: Callable[[cs.En.CSL,
                                                    TYPE,
                                                    Optional[mp_mngr.SyncManager]],
                                                   rcsd.Pr.Value[TYPE]],
                       fix_exec: execs.Im.Exec) -> None:
    init_bool_val: Final[Any] = True
    bool_val: Final[rcsd.Pr.Value[Any]] = fix_value_factory(fix_csl,
                                                            init_bool_val,
                                                            fix_manager)

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

    _c_exec(csl=fix_csl,
            func=__c_value_inv,
            params=((bool_val,), {}),
            join=True)

    assert bool_val.value == (not init_bool_val)

    _c_exec(csl=fix_csl,
            func=__c_value_inv,
            params=(tuple(), {"val_": bool_val}),
            join=True)

    assert bool_val.value == init_bool_val
    assert len(fix_exec.c_get_runners()) == 0


def test_cs_str_value(fix_csl: cs.En.CSL,
                      fix_manager: Optional[mp_mngr.SyncManager],
                      fix_value_factory: Callable[[cs.En.CSL,
                                                   TYPE,
                                                   Optional[mp_mngr.SyncManager]],
                                                  rcsd.Pr.Value[TYPE]],
                      fix_exec: execs.Im.Exec) -> None:

    init_str_val: Final[Any] = "init_str"
    str_val: Final[rcsd.Pr.Value[Any]] = fix_value_factory(fix_csl,
                                                           init_str_val,
                                                           fix_manager)

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

    assert str_val.value == init_str_val
    _c_exec(csl=fix_csl,
            func=__c_value_set_str,
            params=((str_val, "other_str"), {}),
            join=True)

    assert str_val.value == "other_str"
    assert len(fix_exec.c_get_runners()) == 0


def __c_mra_add_int(mra: Union[con.Pr.MutableRandomAccess[Any]],
                    add: int,
                    idx: int) -> None:
    assert len(mra) > idx
    assert isinstance(mra[idx], (int, float))
    mra[idx] = mra[idx] + add


def __c_mra_mult_int(mra: con.Pr.MutableRandomAccess[Any],
                     factor: int,
                     idx: int) -> None:
    assert len(mra) > idx
    assert isinstance(mra[idx], (int, float, str))
    mra[idx] = mra[idx] * factor


def __c_mra_mult_slice_int(mra: con.Pr.MutableRandomAccess[Any],
                           factor: int,
                           sl: slice) -> None:
    x: con.Pr.RandomAccess[Any] = mra[sl]
    assert isinstance(x, con.Pr.MutableRandomAccess) and isinstance(x, con.Pr.Iterable)

    for i in range(len(x)):
        assert isinstance(x[i], (int, float, str))
        x[i] = x[i] * factor

    mra[sl] = x


def __c_mra_index_error(mra: con.Pr.MutableRandomAccess[Any]) -> None:
    with pytest.raises(IndexError):
        mra[len(mra)] = 4


def test_cs_mra(fix_csl: cs.En.CSL,
                fix_manager: Optional[mp_mngr.SyncManager],
                fix_tuple_leq_five: Tuple[Any, ...],
                fix_mra_factory: Callable[[cs.En.CSL,
                                           con.Pr.Iterable[TYPE],
                                           Optional[mp_mngr.SyncManager]],
                                          con.Pr.MutableRandomAccess[TYPE]],
                fix_exec: execs.Im.Exec) -> None:
    assert len(fix_tuple_leq_five) >= 5

    mra: con.Pr.MutableRandomAccess[Any] = fix_mra_factory(fix_csl,
                                                           fix_tuple_leq_five,
                                                           fix_manager)

    assert len(mra) == len(fix_tuple_leq_five)

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

    _c_exec(csl=fix_csl,
            func=__c_mra_add_int,
            params=((mra, 5, 2), {}),
            join=True)

    assert len(mra) == len(fix_tuple_leq_five)
    assert (tuple(mra[i] for i in range(len(mra)))
            == (fix_tuple_leq_five[0:2]
                + (fix_tuple_leq_five[2] + 5,)
                + fix_tuple_leq_five[3:]))

    _c_exec(csl=fix_csl,
            func=__c_mra_mult_int,
            params=((mra, 2, 1), {}),
            join=True)

    assert len(mra) == len(fix_tuple_leq_five)
    assert (tuple(mra[i] for i in range(len(mra)))
            == (fix_tuple_leq_five[0:1]
                + (fix_tuple_leq_five[1] * 2, fix_tuple_leq_five[2] + 5,)
                + fix_tuple_leq_five[3:]))

    _c_exec(csl=fix_csl,
            func=__c_mra_mult_slice_int,
            params=((mra, 3, slice(2, 5)), {}),
            join=True)

    assert len(mra) == len(fix_tuple_leq_five)
    assert (tuple(mra[i] for i in range(len(mra)))
            == (fix_tuple_leq_five[0:1]
                + (fix_tuple_leq_five[1] * 2, (fix_tuple_leq_five[2] + 5) * 3,)
                + tuple(fix_tuple_leq_five[i] * 3
                        for i in range(3, 5))
                + fix_tuple_leq_five[5:]))

    _c_exec(csl=fix_csl,
            func=__c_mra_index_error,
            params=((mra,), {}),
            join=True)

    assert len(fix_exec.c_get_runners()) == 0


def __c_mlseq_append_int(mlseq: con.Pr.MutableLengthSequence[Any],
                         elem: int) -> None:
    mlseq.append(elem)


def __c_mlseq_extend_int(mlseq: con.Pr.MutableLengthSequence[Any],
                         elems: con.Pr.Iterable[int]) -> None:
    mlseq.extend(elems)


def __c_mlseq_set_slice_int(mlseq: con.Pr.MutableLengthSequence[Any],
                            elems: con.Pr.Iterable[int],
                            sl: slice) -> None:
    mlseq[sl] = elems


def __c_mlseq_insert_int(mlseq: con.Pr.MutableLengthSequence[Any],
                         elem: int,
                         idx: int) -> None:
    mlseq.insert(idx, elem)


def __c_mlseq_remove_int(mlseq: con.Pr.MutableLengthSequence[Any],
                         elem: int,
                         all_: bool = False) -> None:
    if all_:
        while True:
            try:
                mlseq.remove(elem)
            except ValueError:
                break
    else:
        mlseq.remove(elem)


def __c_mlseq_del(mlseq: con.Pr.MutableLengthSequence[Any],
                  idx: Union[int, slice]) -> None:
    del mlseq[idx]


def __c_mlseq_pop_int(mlseq: con.Pr.MutableLengthSequence[Any],
                      idx: int) -> None:
    mlseq.pop(idx)


def __c_mlseq_clear(mlseq: con.Pr.MutableLengthSequence[Any]) -> None:
    if isinstance(mlseq, ListProxy):  # For some reason, ListProxy does not have a 'clear()' method
        mlseq[:] = []
    else:
        mlseq.clear()


def test_cs_mlseq(fix_csl: cs.En.CSL,
                  fix_manager: Optional[mp_mngr.SyncManager],
                  fix_tuple_leq_five: Tuple[Any, ...],
                  fix_mlseq_factory: Callable[[cs.En.CSL,
                                               con.Pr.Iterable[TYPE],
                                               Optional[mp_mngr.SyncManager]],
                                              con.Pr.MutableLengthSequence[TYPE]],
                  fix_exec: execs.Im.Exec) -> None:
    assert len(fix_tuple_leq_five) >= 5

    mlseq: con.Pr.MutableLengthSequence[Any] = fix_mlseq_factory(fix_csl,
                                                                 fix_tuple_leq_five,
                                                                 fix_manager)

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

    item_list: List[Any] = list(fix_tuple_leq_five)

    assert len(mlseq) == len(item_list) == len(fix_tuple_leq_five)
    assert all(mlseq[i] == item_list[i] for i in range(len(item_list)))

    _c_exec(csl=fix_csl,
            func=__c_mlseq_append_int,
            params=((mlseq, 5), {}),
            join=True)
    item_list.append(5)

    assert tuple(mlseq) == tuple(item_list)
    assert mlseq.index(5) == item_list.index(5)

    _c_exec(csl=fix_csl,
            func=__c_mlseq_extend_int,
            params=((mlseq, (2, 5)), {}),
            join=True)
    item_list.extend((2, 5))

    assert tuple(mlseq) == tuple(item_list)
    assert mlseq.index(2) == item_list.index(2)

    _c_exec(csl=fix_csl,
            func=__c_mlseq_set_slice_int,
            params=((mlseq, (66, 12, 44, 11), slice(1, 3)), {}),
            join=True)
    item_list[1:3] = (66, 12, 44, 11)

    assert tuple(mlseq) == tuple(item_list)
    assert mlseq.index(12) == item_list.index(12)

    _c_exec(csl=fix_csl,
            func=__c_mlseq_del,
            params=((mlseq, 1), {}),
            join=True)
    del item_list[1]

    assert tuple(mlseq) == tuple(item_list)

    _c_exec(csl=fix_csl,
            func=__c_mlseq_del,
            params=((mlseq, slice(2, 4)), {}),
            join=True)
    del item_list[2:4]

    assert tuple(mlseq) == tuple(item_list)

    _c_exec(csl=fix_csl,
            func=__c_mlseq_insert_int,
            params=((mlseq, 89, 3), {}),
            join=True)
    item_list.insert(3, 89)

    assert tuple(mlseq) == tuple(item_list)
    assert mlseq.index(89) == item_list.index(89)

    _c_exec(csl=fix_csl,
            func=__c_mlseq_remove_int,
            params=((mlseq, 89), {}),
            join=True)
    item_list.remove(89)

    _c_exec(csl=fix_csl,
            func=__c_mlseq_remove_int,
            params=((mlseq, 89, True), {}),
            join=True)
    while True:
        try:
            item_list.remove(89)
        except ValueError:
            break

    assert 89 not in item_list
    assert tuple(mlseq) == tuple(item_list)
    with pytest.raises(ValueError):
        assert mlseq.index(89) == item_list.index(89)

    old_val: Any = mlseq[3]
    _c_exec(csl=fix_csl,
            func=__c_mlseq_pop_int,
            params=((mlseq, 3), {}),
            join=True)

    assert old_val == item_list.pop(3)
    assert tuple(mlseq) == tuple(item_list)

    _c_exec(csl=fix_csl,
            func=__c_mlseq_clear,
            params=((mlseq, ), {}),
            join=True)
    item_list.clear()

    assert len(item_list) == len(mlseq) == 0
    assert tuple(mlseq) == tuple(item_list)
    assert len(fix_exec.c_get_runners()) == 0


def __c_mm_set(mm: con.Pr.MutableMapping[Any, Any],
               key: Any,
               value: Any) -> None:
    assert key not in mm
    mm[key] = value


def __c_mm_del(mm: con.Pr.MutableMapping[Any, Any],
               key: Any) -> None:
    assert key in mm
    del mm[key]


def __c_mm_pop(mm: con.Pr.MutableMapping[Any, Any],
               key: Any) -> None:
    assert key in mm
    assert mm[key] == mm.pop(key)
    assert key not in mm


def __c_mm_pop_default(mm: con.Pr.MutableMapping[Any, Any],
                       key: Any) -> None:
    assert key in mm

    default: int = 0
    while default in mm.values():
        default += 1
    assert default not in mm

    assert mm.pop(key, default) != default
    assert mm.pop(key, default) == default


def __c_mm_popitem(mm: con.Pr.MutableMapping[Any, Any]) -> None:
    mm.popitem()


def __c_mm_update_with_mapping(mm: con.Pr.MutableMapping[Any, Any],
                               mapping: con.Pr.Mapping[Any, Any]) -> None:
    mm.update(mapping)


def __c_mm_update_with_iterable(mm: con.Pr.MutableMapping[Any, Any],
                                itbl: con.Pr.Iterable[Tuple[Any, Any]]) -> None:
    mm.update(itbl)


def __c_mm_update_with_kw(mm: con.Pr.MutableMapping[Any, Any],
                          **kwargs: Any) -> None:
    mm.update(**kwargs)


def __c_mm_setdefault(mm: con.Pr.MutableMapping[Any, Any],
                      key: Any,
                      default: Any) -> None:
    mm.setdefault(key, default)


def __c_mm_clear(mm: con.Pr.MutableMapping[Any, Any]) -> None:
    mm.clear()


def test_cs_mm(fix_csl: cs.En.CSL,
               fix_manager: Optional[mp_mngr.SyncManager],
               fix_mapping: con.Pr.Mapping[Any, Any],
               fix_mm_factory: Callable[[cs.En.CSL,
                                         con.Pr.Mapping[K_TYPE, V_TYPE],
                                         Optional[mp_mngr.SyncManager]],
                                        con.Pr.MutableMapping[K_TYPE, V_TYPE]],
               fix_exec: execs.Im.Exec) -> None:

    mm: con.Pr.MutableMapping[Any, Any] = fix_mm_factory(fix_csl,
                                                         fix_mapping,
                                                         fix_manager)

    mm_dict: Dict[Any, Any] = dict(fix_mapping)

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

    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())

    _c_exec(csl=fix_csl,
            func=__c_mm_set,
            params=((mm, 321513124156, "asdfasdfasdf"), {}),
            join=True)
    mm_dict[321513124156] = "asdfasdfasdf"

    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())

    key: Any = next(iter(mm))
    _c_exec(csl=fix_csl,
            func=__c_mm_del,
            params=((mm, key), {}),
            join=True)
    del mm_dict[key]

    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())

    key = next(iter(mm))
    _c_exec(csl=fix_csl,
            func=__c_mm_pop,
            params=((mm, key), {}),
            join=True)
    mm_dict.pop(key)

    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())

    key = next(iter(mm))
    _c_exec(csl=fix_csl,
            func=__c_mm_pop_default,
            params=((mm, key), {}),
            join=True)
    mm_dict.pop(key)

    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())

    key = next(iter(mm))
    _c_exec(csl=fix_csl,
            func=__c_mm_update_with_mapping,
            params=((mm, {1112123: 3412, "cvbnn": 351, key: 99959959595}), {}),
            join=True)
    mm_dict.update({1112123: 3412, "cvbnn": 351, key: 99959959595})

    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())

    key = next(iter(mm))
    _c_exec(csl=fix_csl,
            func=__c_mm_update_with_iterable,
            params=((mm, ((1999123, 32212), (key, 9995422), ("cbbaddxvbnn", 151))), {}),
            join=True)
    mm_dict.update(((1999123, 32212), (key, 9995422), ("cbbaddxvbnn", 151)))

    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())

    _c_exec(csl=fix_csl,
            func=__c_mm_update_with_kw,
            params=((mm,), {"lglgk": 3331, "axxx": 456}),
            join=True)
    mm_dict.update(lglgk=3331, axxx=456)

    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())

    key = next(iter(mm))
    _c_exec(csl=fix_csl,
            func=__c_mm_setdefault,
            params=((mm, key, 1020202), {}),
            join=True)
    mm_dict.setdefault(key, 2244516667)
    assert 2244516667 != mm_dict[key] == mm[key] != 102020
    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())

    key = 0
    while key in mm:
        key += 1
    assert key not in mm and key not in mm_dict

    _c_exec(csl=fix_csl,
            func=__c_mm_setdefault,
            params=((mm, key, 6678432), {}),
            join=True)
    mm_dict.setdefault(key, 6678432)
    assert mm_dict[key] == mm[key] == 6678432
    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())

    _c_exec(csl=fix_csl,
            func=__c_mm_clear,
            params=((mm,), {}),
            join=True)
    mm_dict.clear()
    assert len(mm) == len(mm_dict) == 0
    assert set(mm) == set(mm.keys()) == set(mm_dict.keys())
    assert set(mm.values()) == set(mm_dict.values())
    assert set(mm.items()) == set(mm_dict.items())
    assert len(fix_exec.c_get_runners()) == 0


def __c_queue_put(queue: con.Pr.Queue[Any],
                  elem: Any,
                  expect_full: bool,
                  blocking: bool,
                  timeout: Optional[float],
                  delay: Optional[float] = None) -> None:

    if delay is not None:
        time.sleep(delay)

    if expect_full:
        with pytest.raises(Full):
            queue.put(elem,
                      blocking,
                      timeout)
    else:
        queue.put(elem,
                  blocking,
                  timeout)


def __c_queue_put_nowait(queue: con.Pr.Queue[Any],
                         elem: Any,
                         expect_full: bool,
                         delay: Optional[float] = None) -> None:

    if delay is not None:
        time.sleep(delay)

    if expect_full:
        with pytest.raises(Full):
            queue.put_nowait(elem)
    else:
        queue.put_nowait(elem)


def __c_queue_get(queue: con.Pr.Queue[Any],
                  expect_empty: bool,
                  blocking: bool,
                  timeout: Optional[float],
                  delay: Optional[float] = None) -> None:

    if delay is not None:
        time.sleep(delay)

    if expect_empty:
        with pytest.raises(Empty):
            queue.get(blocking,
                      timeout)
    else:
        queue.get(blocking,
                  timeout)


def __c_queue_get_nowait(queue: con.Pr.Queue[Any],
                         expect_empty: bool,
                         delay: Optional[float] = None) -> None:

    if delay is not None:
        time.sleep(delay)

    if expect_empty:
        with pytest.raises(Empty):
            queue.get_nowait()
    else:
        queue.get_nowait()


def test_cs_queue_no_cap(fix_csl: cs.En.CSL,
                         fix_manager: Optional[mp_mngr.SyncManager],
                         fix_init_queue: con.Pr.SizedIterable[Any],
                         fix_queue_factory: Callable[[cs.En.CSL,
                                                      con.Pr.SizedIterable[TYPE],
                                                      Optional[int],
                                                      Optional[mp_mngr.SyncManager]],
                                                     con.Pr.Queue[TYPE]],
                         fix_exec: execs.Im.Exec) -> None:
    fix_init_queue = con.Ca.c_to_seq(fix_init_queue)

    queue: con.Pr.Queue[Any] = fix_queue_factory(fix_csl,
                                                 fix_init_queue,
                                                 None,
                                                 fix_manager)

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

    queue_deque: con.Pr.Queue[Any] = con.Im.DequeQueue(capacity=None)
    for item in fix_init_queue:
        queue_deque.put(item)

    _c_exec(csl=fix_csl,
            func=__c_queue_put,
            params=((queue, 32, False, False, None), {}),
            join=True)
    queue_deque.put(32, False, None)
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 1
    assert not queue.full() and not queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    _c_exec(csl=fix_csl,
            func=__c_queue_put,
            params=((queue, "asdfggasdahf", False, False, None), {}),
            join=True)
    queue_deque.put("asdfggasdahf", False, None)
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 2
    assert not queue.full() and not queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    _c_exec(csl=fix_csl,
            func=__c_queue_put_nowait,
            params=((queue, 2345.63, False), {}),
            join=True)
    queue_deque.put_nowait(2345.63)
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 3
    assert not queue.full() and not queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    _c_exec(csl=fix_csl,
            func=__c_queue_put_nowait,
            params=((queue, "ccxdfasdf", False), {}),
            join=True)
    queue_deque.put_nowait("ccxdfasdf")
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 4
    assert not queue.full() and not queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    _c_exec(csl=fix_csl,
            func=__c_queue_get,
            params=((queue, False, False, None), {}),
            join=True)
    queue_deque.get(False, None)
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 3
    assert not queue.full() and not queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    _c_exec(csl=fix_csl,
            func=__c_queue_get_nowait,
            params=((queue, False), {}),
            join=True)
    queue_deque.get_nowait()
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 2
    assert not queue.full() and not queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    for i in range(len(fix_init_queue) + 1, -1, -1):
        assert queue.get_nowait() == queue_deque.get_nowait()
        assert queue.qsize() == queue_deque.qsize() == i
        assert not queue.full() and not queue_deque.full()
        assert (queue.qsize() == 0) == queue.empty() == queue_deque.empty()

    de_assert_duration(min_sec=2.0, max_sec=3.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_queue_get,
                                                                 params=((queue, True, True, 2.0), {}),
                                                                 join=True))()  # type: ignore
    with pytest.raises(Empty):
        de_assert_duration(min_sec=2.0, max_sec=3.0)(
            lambda: queue_deque.get(True, 2.0))()  # type: ignore
    assert queue.qsize() == queue_deque.qsize() == 0

    de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_queue_get_nowait,
                                                                 params=((queue, True), {}),
                                                                 join=True))()  # type: ignore
    with pytest.raises(Empty):
        de_assert_duration(min_sec=0.0, max_sec=1.0)(
            lambda: queue_deque.get_nowait())()  # type: ignore
    assert queue.qsize() == queue_deque.qsize() == 0

    assert queue.empty() and queue_deque.empty() and not queue.full() and not queue_deque.full()
    assert len(fix_exec.c_get_runners()) == 0


def test_cs_queue_cap(fix_csl: cs.En.CSL,
                      fix_manager: Optional[mp_mngr.SyncManager],
                      fix_init_queue: con.Pr.SizedIterable[Any],
                      fix_queue_factory: Callable[[cs.En.CSL,
                                                   con.Pr.SizedIterable[TYPE],
                                                   Optional[int],
                                                   Optional[mp_mngr.SyncManager]],
                                                  con.Pr.Queue[TYPE]],
                      fix_exec: execs.Im.Exec) -> None:
    fix_init_queue = con.Ca.c_to_seq(fix_init_queue)

    queue: con.Pr.Queue[Any] = fix_queue_factory(fix_csl,
                                                 fix_init_queue,
                                                 len(fix_init_queue) + 2,
                                                 fix_manager)

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

    queue_deque: con.Pr.Queue[Any] = con.Im.DequeQueue(capacity=len(fix_init_queue) + 2)
    for item in fix_init_queue:
        queue_deque.put(item)

    _c_exec(csl=fix_csl,
            func=__c_queue_put,
            params=((queue, 32, False, False, None), {}),
            join=True)

    queue_deque.put(32, False, None)
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 1
    assert not queue.full() and not queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    _c_exec(csl=fix_csl,
            func=__c_queue_put,
            params=((queue, "asdfggasdahf", False, False, None), {}),
            join=True)
    queue_deque.put("asdfggasdahf", False, None)
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 2
    assert queue.full() and queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    de_assert_duration(min_sec=None, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                  func=__c_queue_put_nowait,
                                                                  params=((queue, 2345.63, True), {}),
                                                                  join=True))()  # type: ignore
    with pytest.raises(Full):
        de_assert_duration(min_sec=None, max_sec=1.0)(
            lambda: queue_deque.put_nowait(2345.63))()  # type: ignore
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 2
    assert queue.full() and queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    de_assert_duration(min_sec=None, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                  func=__c_queue_put,
                                                                  params=((queue, "ccxdfasdf", True, False, None), {}),
                                                                  join=True))()  # type: ignore
    with pytest.raises(Full):
        de_assert_duration(min_sec=None, max_sec=1.0)(
            lambda: queue_deque.put("ccxdfasdf", False, None))()  # type: ignore
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 2
    assert queue.full() and queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    de_assert_duration(min_sec=2.0, max_sec=3.0)(lambda: _c_exec(csl=fix_csl,
                                                                 func=__c_queue_put,
                                                                 params=((queue, "ccxdfasdf", True, True, 2.0), {}),
                                                                 join=True))()  # type: ignore
    with pytest.raises(Full):
        de_assert_duration(min_sec=2.0, max_sec=3.0)(
            lambda: queue_deque.put("ccxdfasdf", True, 2.0))()  # type: ignore
    assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 2
    assert queue.full() and queue_deque.full()
    assert not queue.empty() and not queue_deque.empty()

    with pytest.raises(TooLongDuration):
        de_assert_duration(min_sec=0.0, max_sec=1.0)(lambda: _c_exec(csl=fix_csl,
                                                                     func=__c_queue_put,
                                                                     params=((queue, "ccxdfasdf", True, True, 2.0), {}),
                                                                     join=True))()  # type: ignore

    with pytest.raises(TooShortDuration):
        de_assert_duration(min_sec=4.0, max_sec=5.0)(lambda: _c_exec(csl=fix_csl,
                                                                     func=__c_queue_put,
                                                                     params=((queue, "ccxdfasdf", True, True, 2.0), {}),
                                                                     join=True))()  # type: ignore

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        _c_exec(csl=fix_csl,
                func=__c_queue_get_nowait,
                params=((queue, False, 4.0), {}),
                join=False)
        de_assert_duration(min_sec=3.0, max_sec=5.0)(lambda: _c_exec(csl=fix_csl,
                                                                     func=__c_queue_put,
                                                                     params=((queue, "vaxxyxy", False, True, None), {}),
                                                                     join=True))()  # type: ignore
        queue_deque.get_nowait()
        queue_deque.put("vaxxyxy", False, None)
        assert queue.qsize() == queue_deque.qsize() == len(fix_init_queue) + 2
        assert queue.full() and queue_deque.full()
        assert not queue.empty() and not queue_deque.empty()

    for i in range(len(fix_init_queue) + 1, -1, -1):
        assert queue.get_nowait() == queue_deque.get_nowait()
        assert queue.qsize() == queue_deque.qsize() == i
        assert not queue.full() and not queue_deque.full()
        assert (queue.qsize() == 0) == queue.empty() == queue_deque.empty()

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        _c_exec(csl=fix_csl,
                func=__c_queue_put_nowait,
                params=((queue, "bvsdh", False, 4.0), {}),
                join=False)
        de_assert_duration(min_sec=3.0, max_sec=5.0)(lambda: _c_exec(csl=fix_csl,
                                                                     func=__c_queue_get,
                                                                     params=((queue, False, True, None), {}),
                                                                     join=True))()  # type: ignore
        queue_deque.put("bvsdh", False, None)
        queue_deque.get_nowait()
        assert queue.qsize() == queue_deque.qsize() == 0
        assert not queue.full() and not queue_deque.full()
        assert queue.empty() and queue_deque.empty()

    # Without joining, manager might get shutdown while
    # unjoined processes might still be running resulting in a Pipe error
    fix_exec.c_join()
