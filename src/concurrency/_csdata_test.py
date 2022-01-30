import multiprocessing.managers as mp_mngr
import time
from queue import Full, Empty
from typing import Optional, Callable, Final, Any, Tuple, cast, Union, overload, List

import pytest

from concurrency._fixtures import fix_csl, fix_manager, fix_picklable_manager,\
    fix_csdata_factory, fix_exec, fix_init_csdata, \
    fix_cscapacityqueue_factory, fix_init_queue, fix_cschunkcapacityqueue_factory, fix_csmseq_factory, \
    fix_csmlseq_factory, fix_init_csmseqdata

from concurrency import cs, csdata, execs
from utils.types.casts import c_assert_not_none
from utils.types.typevars import TYPE, TYPE2
from utils.functional import tools as ft

import utils.types.containers as con

INT_VAL: Final[int] = 333
ADD_X: Final[int] = 2
APPLY_TIMEOUT: Final[float] = 8.0


def __apply_add_x(num: int) -> Tuple[int, Tuple[str, int]]:
    return num + ADD_X, (__apply_add_x.__name__, num)


def __apply_add_x_timeout(num: int) -> Tuple[int, Tuple[str, int]]:
    time.sleep(APPLY_TIMEOUT)
    return num + ADD_X, (__apply_add_x.__name__, num)


# noinspection PyArgumentList
def test_csdata(fix_csl: cs.En.CSL,
                fix_init_csdata: Any,
                fix_manager: Optional[mp_mngr.SyncManager],
                fix_csdata_factory: Callable[[cs.En.CSL,
                                              TYPE,
                                              Optional[mp_mngr.SyncManager]],
                                             csdata.Pr.CSData[TYPE]],
                fix_exec: execs.Im.Exec) -> None:

    cs_data: Final[csdata.Pr.CSData[Any]] = fix_csdata_factory(fix_csl,
                                                               fix_init_csdata,
                                                               fix_manager)

    execs.Ca.c_exec_multiple(
        csl=fix_csl,
        exec_=fix_exec,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {'condition_check': ft.c_eq,
                             'params': ((), {'el_1': (cs_data,
                                                      cs_data.c_get.__name__),
                                             'params_1': None,
                                             'el_2': ft.c_identity,
                                             'params_2': ((), {'el': fix_init_csdata})}),
                             'max_check_count': 1}),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=(cs_data,
                                  cs_data.c_set.__name__),
                params=((), {'new_val': INT_VAL}),
                join=True),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {'condition_check': ft.c_eq,
                             'params': ((), {'el_1': (cs_data,
                                                      cs_data.c_get.__name__),
                                             'params_1': None,
                                             'el_2': ft.c_identity,
                                             'params_2': ((), {'el': INT_VAL})}),
                             'max_check_count': 1}),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {'condition_check': ft.c_eq,
                             'params': ((), {'el_1': (cs_data,
                                                      cs_data.c_apply.__name__),
                                             'params_1': ((), {
                                                 'func': __apply_add_x
                                             }),
                                             'el_2': ft.c_identity,
                                             'params_2': ((), {'el': (__apply_add_x.__name__,
                                                                      INT_VAL)})}),
                             'max_check_count': 1}),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {'condition_check': ft.c_eq,
                             'params': ((), {'el_1': (cs_data,
                                                      cs_data.c_get.__name__),
                                             'params_1': None,
                                             'el_2': ft.c_identity,
                                             'params_2': ((), {'el': INT_VAL + ADD_X})}),
                             'max_check_count': 1}),
                join=True
            )
        ],
        manager=fix_manager)

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        # noinspection PyArgumentList
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {'condition_check': ft.c_eq,
                                 'params': ((), {'el_1': (cs_data,
                                                          cs_data.c_apply.__name__),
                                                 'params_1': ((), {
                                                     'func': __apply_add_x_timeout
                                                 }),
                                                 'el_2': ft.c_identity,
                                                 'params_2': ((), {'el': (__apply_add_x.__name__,
                                                                          INT_VAL + ADD_X)})}),
                                 'max_check_count': 1}),
                    join=False
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {'condition_check': (cs_data.c_get_csrwlock(),
                                                                     cs_data.c_get_csrwlock().c_is_held.__name__),
                                                 'params': ((), {}),
                                                 'max_duration': 1.0}),
                                    join=True),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {'condition_check': ft.c_eq,
                                 'params': ((), {'el_1': (cs_data,
                                                          cs_data.c_get.__name__),
                                                 'params_1': None,
                                                 'el_2': ft.c_identity,
                                                 'params_2': ((), {'el': INT_VAL + 2 * ADD_X})}),
                                 'max_check_count': 1}),
                    join=True,
                    min_sec=APPLY_TIMEOUT - 2.0,
                    max_sec=APPLY_TIMEOUT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT,
            _max_sec=APPLY_TIMEOUT + 1.0)

        # noinspection PyArgumentList
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {'condition_check': ft.c_eq,
                                 'params': ((), {'el_1': (cs_data,
                                                          cs_data.c_apply.__name__),
                                                 'params_1': ((), {
                                                     'func': __apply_add_x_timeout
                                                 }),
                                                 'el_2': ft.c_identity,
                                                 'params_2': ((), {'el': (__apply_add_x.__name__,
                                                                          INT_VAL + 2 * ADD_X)})}),
                                 'max_check_count': 1}),
                    join=False
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {'condition_check': (cs_data.c_get_csrwlock(),
                                                                     cs_data.c_get_csrwlock().c_is_held.__name__),
                                                 'params': ((), {}),
                                                 'max_duration': 1.0}),
                                    join=True),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=(cs_data,
                                      cs_data.c_set.__name__),
                    params=((), {'new_val': INT_VAL}),
                    join=True,
                    min_sec=APPLY_TIMEOUT - 2.0,
                    max_sec=APPLY_TIMEOUT + 1.0
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {'condition_check': ft.c_eq,
                                 'params': ((), {'el_1': (cs_data,
                                                          cs_data.c_get.__name__),
                                                 'params_1': None,
                                                 'el_2': ft.c_identity,
                                                 'params_2': ((), {'el': INT_VAL})}),
                                 'max_check_count': 1}),
                    join=True
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT,
            _max_sec=APPLY_TIMEOUT + 1.0)

    fix_exec.c_join()


# noinspection PyArgumentList
def test_csmseqdata_non_concurrent(
        fix_csl: cs.En.CSL,
        fix_init_csmseqdata: con.Pr.Iterable[Any],
        fix_manager: Optional[mp_mngr.SyncManager],
        fix_csmseq_factory: Callable[[cs.En.CSL,
                                      con.Pr.Iterable[TYPE],
                                      Optional[mp_mngr.SyncManager]],
                                     csdata.Pr.CSMutableSequence[TYPE]],
        fix_exec: execs.Im.Exec) -> None:

    fix_init_csmseqdata = tuple(fix_init_csmseqdata)
    init_len: Final[int] = len(fix_init_csmseqdata)
    assert init_len > 2

    csmseq: csdata.Pr.CSMutableSequence[Any] = cast(csdata.Pr.CSMutableSequence[Any],
                                                    fix_csmseq_factory(fix_csl,
                                                                       fix_init_csmseqdata,
                                                                       fix_manager))

    # getitem int index
    for getitem_name in (csmseq.__getitem__.__name__, csmseq.c_get.__name__):
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     getitem_name),
                            'params_1': ((init_len-1, ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': fix_init_csmseqdata[init_len-1]
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     getitem_name),
                            'params_1': ((-init_len, ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': fix_init_csmseqdata[-init_len]
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     getitem_name),
                            'params_1': ((0, ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': fix_init_csmseqdata[0]
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       getitem_name),
                            'params': ((init_len, ), None),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       getitem_name),
                            'params': ((-(init_len + 1), ), None),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       getitem_name),
                            'params': ((-(init_len + 10), ), None),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       getitem_name),
                            'params': ((init_len + 9, ), None),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                )
            ],
            manager=fix_manager)

    # getitem slice index
    for getitem_name in (csmseq.__getitem__.__name__, csmseq.c_get.__name__):
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     getitem_name),
                            'params_1': ((slice(0, None), ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': fix_init_csmseqdata
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     getitem_name),
                            'params_1': ((slice(0, None), ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': fix_init_csmseqdata[0:]
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     getitem_name),
                            'params_1': ((slice(1, 23), ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': fix_init_csmseqdata[1:23]
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     getitem_name),
                            'params_1': ((slice(-100, 23), ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': fix_init_csmseqdata[-100:23]
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     getitem_name),
                            'params_1': ((slice(-3, -1), ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': fix_init_csmseqdata[-3:-1]
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     getitem_name),
                            'params_1': ((slice(-1, -3), ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': fix_init_csmseqdata[-1:-3]
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                )
            ],
            manager=fix_manager)

    # setitem int index
    for setitem_name, offset in ((csmseq.__setitem__.__name__, 2), (csmseq.c_set.__name__, 1)):
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=(csmseq,
                                      setitem_name),
                    params=((init_len - offset,
                             FILL_ELEM), {}),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__getitem__.__name__),
                            'params_1': ((init_len - offset, ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': FILL_ELEM
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                )
            ],
            manager=fix_manager)

    # setitem int index
    for setitem_name, offset in ((csmseq.__setitem__.__name__, 2), (csmseq.c_set.__name__, 1)):
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=(csmseq,
                                      setitem_name),
                    params=((init_len - offset,
                             FILL_ELEM), {}),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__getitem__.__name__),
                            'params_1': ((init_len - offset, ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': FILL_ELEM
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=(csmseq,
                                      setitem_name),
                    params=((-init_len,
                             FILL_ELEM), {}),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__getitem__.__name__),
                            'params_1': ((-init_len, ), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': FILL_ELEM
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       setitem_name),
                            'params': ((init_len, "SOMEVAL"), None),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       setitem_name),
                            'params': ((-(init_len + 1), "SOMEVAL"), None),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                )
            ],
            manager=fix_manager)

    # setitem slice - unchanged length
    for setitem_name in (csmseq.__setitem__.__name__, csmseq.c_set.__name__):
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=(csmseq,
                                      setitem_name),
                    params=((slice(1, 3),
                             tuple(range(55, 57))), {}),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__getitem__.__name__),
                            'params_1': ((slice(1, 3),), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': tuple(range(55, 57))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=(csmseq,
                                      setitem_name),
                    params=((slice(-(init_len + 10000), 1),
                             (1233123123123123123, )), {}),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__getitem__.__name__),
                            'params_1': ((slice(-(init_len + 10000), 1),), None),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': (1233123123123123123,)
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
            ],
            manager=fix_manager)

    if not isinstance(csmseq, con.Pr.MutableLengthSequence):
        # setitem slice - changed length - expect IndexError
        for setitem_name in (csmseq.__setitem__.__name__, csmseq.c_set.__name__):
            execs.Ca.c_exec_multiple(
                csl=fix_csl,
                exec_=fix_exec,
                join=False,
                exec_params=[
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_raises,
                            'params': ((), {
                                'raiser': (csmseq,
                                           setitem_name),
                                'params': ((slice(1, 3), (33123123,)), None),
                                'expected_exception': IndexError
                            }),
                            'max_check_count': 1
                        }),
                        join=True
                    ),
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_raises,
                            'params': ((), {
                                'raiser': (csmseq,
                                           setitem_name),
                                'params': ((slice(1, 2), (33123123, "asdfasdf")), None),
                                'expected_exception': IndexError
                            }),
                            'max_check_count': 1
                        }),
                        join=True
                    ),
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_raises,
                            'params': ((), {
                                'raiser': (csmseq,
                                           setitem_name),
                                'params': ((slice(init_len, init_len + 1), (33123123, )), None),
                                'expected_exception': IndexError
                            }),
                            'max_check_count': 1
                        }),
                        join=True
                    ),
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_raises,
                            'params': ((), {
                                'raiser': (csmseq,
                                           setitem_name),
                                'params': ((slice(-init_len - 10000, 0), (33123123, 3223123)), None),
                                'expected_exception': IndexError
                            }),
                            'max_check_count': 1
                        }),
                        join=True
                    )
                ],
                manager=fix_manager)

    # len
    execs.Ca.c_exec_multiple(
        csl=fix_csl,
        exec_=fix_exec,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.__len__.__name__),
                        'params_1': None,
                        'el_2': ft.c_identity,
                        'params_2': ((len(fix_init_csmseqdata),), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.c_len.__name__),
                        'params_1': None,
                        'el_2': ft.c_identity,
                        'params_2': ((len(fix_init_csmseqdata),), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            )
        ],
        manager=fix_manager)

    # reset
    csmseq[0:] = fix_init_csmseqdata
    assert (l := len(csmseq)) == len(fix_init_csmseqdata) and all(csmseq[i] == fix_init_csmseqdata[i]
                                                                  for i in range(l))

    # iter and reversed
    execs.Ca.c_exec_multiple(
        csl=fix_csl,
        exec_=fix_exec,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.__iter__.__name__),
                        'params_1': None,
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': fix_init_csmseqdata
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.__reversed__.__name__),
                        'params_1': None,
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': reversed(fix_init_csmseqdata)
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            )
        ],
        manager=fix_manager)

    # contains
    execs.Ca.c_exec_multiple(
        csl=fix_csl,
        exec_=fix_exec,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': (csmseq,
                                        csmseq.__contains__.__name__),
                    'params': ((fix_init_csmseqdata[-1], ), None),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': (csmseq,
                                        csmseq.__contains__.__name__),
                    'params': ((fix_init_csmseqdata[1], ), None),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_not,
                    'params': ((), {
                        'condition_check': (csmseq,
                                            csmseq.__contains__.__name__),
                        'params': (("I AM CERTAINLY NOT CONTAINED",), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            )
        ],
        manager=fix_manager)

    # index
    execs.Ca.c_exec_multiple(
        csl=fix_csl,
        exec_=fix_exec,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.index.__name__),
                        'params_1': ((fix_init_csmseqdata[0], ), None),
                        'el_2': ft.c_identity,
                        'params_2': ((0,), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.index.__name__),
                        'params_1': ((fix_init_csmseqdata[1], 1, 2), None),
                        'el_2': ft.c_identity,
                        'params_2': ((1,), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_raises,
                    'params': ((), {
                        'raiser': (csmseq,
                                   csmseq.index.__name__),
                        'params': (("I AM CERTAINLY NOT CONTAINED", 2, 44), None),
                        'expected_exception': ValueError
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_raises,
                    'params': ((), {
                        'raiser': (csmseq,
                                   csmseq.index.__name__),
                        'params': ((fix_init_csmseqdata[0], init_len, init_len + 12), None),
                        'expected_exception': ValueError
                    }),
                    'max_check_count': 1
                }),
                join=True
            )
        ],
        manager=fix_manager)

    # count
    execs.Ca.c_exec_multiple(
        csl=fix_csl,
        exec_=fix_exec,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.count.__name__),
                        'params_1': ((fix_init_csmseqdata[2], ), None),
                        'el_2': ft.c_identity,
                        'params_2': ((fix_init_csmseqdata.count(fix_init_csmseqdata[2]),), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.count.__name__),
                        'params_1': ((fix_init_csmseqdata[0],), None),
                        'el_2': ft.c_identity,
                        'params_2': ((fix_init_csmseqdata.count(fix_init_csmseqdata[0]),), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.count.__name__),
                        'params_1': (("I AM CERTAINLY NOT CONTAINED",), None),
                        'el_2': ft.c_identity,
                        'params_2': ((0,), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            )
        ],
        manager=fix_manager)

    fix_exec.c_join()


APPLY_RETURN: Final[str] = "APPLY_RETURN"

APPLY_TIMEOUT_SHORT: Final[float] = 5.0


def __apply_to_str(old_val: Any) -> Tuple[Any, str]:
    return str(old_val), APPLY_RETURN


def __apply_to_str_seq(old_vals: con.Pr.RandomAccess[Any]) -> Tuple[con.Pr.Iterable[Any], str]:
    return tuple(str(elem)
                 for elem in con.Ca.c_to_itbl(obj=old_vals)), APPLY_RETURN


def __apply_to_str_seq_incr_len(old_vals: con.Pr.RandomAccess[Any]) -> Tuple[con.Pr.Iterable[Any], str]:
    return tuple(str(elem)
                 for elem in con.Ca.c_to_itbl(obj=old_vals)) + ("Nope",), APPLY_RETURN


def __apply_to_str_seq_decr_len(old_vals: con.Pr.RandomAccess[Any]) -> Tuple[con.Pr.Iterable[Any], str]:
    assert len(old_vals) > 0
    return tuple(str(old_vals[i])
                 for i in range(len(old_vals) - 1)), APPLY_RETURN


def __apply_delay(old_val: Any) -> Tuple[Any, str]:
    time.sleep(APPLY_TIMEOUT_SHORT)

    return old_val, APPLY_RETURN


def __apply_no_delay(old_val: Any) -> Tuple[Any, str]:
    return old_val, APPLY_RETURN


@overload
def __apply_helper(mseq: con.Pr.MutableSequence[TYPE],
                   index: int,
                   func: Callable[[TYPE], Tuple[TYPE, TYPE2]]) -> TYPE2:
    ...


@overload
def __apply_helper(mseq: con.Pr.MutableSequence[TYPE],
                   index: slice,
                   func: Callable[[con.Pr.RandomAccess[TYPE]], Tuple[con.Pr.Iterable[TYPE], TYPE2]]) -> TYPE2:
    ...


def __apply_helper(mseq: con.Pr.MutableSequence[TYPE],
                   index: Union[int, slice],
                   func: Union[Callable[[con.Pr.RandomAccess[TYPE]], Tuple[con.Pr.Iterable[TYPE], TYPE2]],
                               Callable[[TYPE], Tuple[TYPE, TYPE2]]]) -> TYPE2:

    if isinstance(index, int):
        func = cast(Callable[[TYPE], Tuple[TYPE, TYPE2]], func)

        old_vals = mseq[index]
        new_vals = func(old_vals)

        mseq[index] = new_vals[0]
        return new_vals[1]
    else:
        assert isinstance(index, slice)
        func = cast(Callable[[con.Pr.RandomAccess[TYPE]], Tuple[con.Pr.Iterable[TYPE], TYPE2]], func)

        old_vals_ = mseq[index]
        new_vals_ = func(old_vals_)

        mseq[index] = new_vals_[0]
        return new_vals_[1]


# noinspection PyArgumentList
def test_csmseqdata_concurrent_apply(
        fix_csl: cs.En.CSL,
        fix_init_csmseqdata: con.Pr.Iterable[Any],
        fix_manager: Optional[mp_mngr.SyncManager],
        fix_csmseq_factory: Callable[[cs.En.CSL,
                                      con.Pr.Iterable[TYPE],
                                      Optional[mp_mngr.SyncManager]],
                                     csdata.Pr.CSMutableSequence[TYPE]],
        fix_exec: execs.Im.Exec) -> None:

    fix_init_csmseqdata = tuple(fix_init_csmseqdata)
    init_len: Final[int] = len(fix_init_csmseqdata)
    assert init_len > 2

    csmseq: csdata.Pr.CSMutableSequence[Any] = cast(csdata.Pr.CSMutableSequence[Any],
                                                    fix_csmseq_factory(fix_csl,
                                                                       fix_init_csmseqdata,
                                                                       fix_manager))

    # apply
    execs.Ca.c_exec_multiple(
        csl=fix_csl,
        exec_=fix_exec,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.c_apply.__name__),
                        'params_1': ((), {
                            'index': 0,
                            'func': __apply_to_str
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((APPLY_RETURN,), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.c_get.__name__),
                        'params_1': ((0,), None),
                        'el_2': ft.c_identity,
                        'params_2': ((str(fix_init_csmseqdata[0]),), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.c_apply.__name__),
                        'params_1': ((), {
                            'index': slice(1, 3),
                            'func': __apply_to_str_seq
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((APPLY_RETURN,), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': (csmseq,
                                 csmseq.c_get.__name__),
                        'params_1': ((slice(1, 3),), None),
                        'el_2': ft.c_identity,
                        'params_2': ((tuple(str(item)
                                            for item in fix_init_csmseqdata[1:3]),), None)
                    }),
                    'max_check_count': 1
                }),
                join=True
            )
        ],
        manager=fix_manager)

    if not isinstance(csmseq, con.Pr.MutableLengthSequence):
        # apply length change
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': slice(1, 3),
                                'func': __apply_to_str_seq_incr_len
                            }),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': slice(1, 3),
                                'func': __apply_to_str_seq_decr_len
                            }),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                )
            ],
            manager=fix_manager)

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        csmseq[0:] = fix_init_csmseqdata

        # test_blocking - apply
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_no_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        # test_blocking - getitem
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__getitem__.__name__),
                            'params_1': ((0, ), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((fix_init_csmseqdata[0],), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        # test_blocking - c_get
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_get.__name__),
                            'params_1': ((0, ), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((fix_init_csmseqdata[0],), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        # test_blocking - setitem
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__setitem__.__name__),
                            'params_1': ((0, "A"), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((None,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        csmseq[0:] = fix_init_csmseqdata
        # test_blocking - c_set
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_set.__name__),
                            'params_1': ((0, "A"), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((None,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        csmseq[0:] = fix_init_csmseqdata

        # test_blocking - __iter__
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__iter__.__name__),
                            'params_1': ((), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((fix_init_csmseqdata,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        # test_blocking - __reversed__
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__reversed__.__name__),
                            'params_1': ((), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((reversed(fix_init_csmseqdata),), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        # test_blocking - contains
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__contains__.__name__),
                            'params_1': ((fix_init_csmseqdata[0],), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((True,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        # test_blocking - len
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.__len__.__name__),
                            'params_1': ((), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((len(fix_init_csmseqdata),), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        # test_blocking - c_len
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_len.__name__),
                            'params_1': ((), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((len(fix_init_csmseqdata),), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        # test_blocking - count
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.count.__name__),
                            'params_1': ((fix_init_csmseqdata[0],), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((fix_init_csmseqdata.count(fix_init_csmseqdata[0]),), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

        # test_blocking - index
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmseq,
                                       csmseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmseq,
                                     csmseq.index.__name__),
                            'params_1': ((fix_init_csmseqdata[0],), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((fix_init_csmseqdata.index(fix_init_csmseqdata[0]),), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )

    fix_exec.c_join()


# noinspection PyArgumentList
def test_csmlseqdata_non_concurrent(
        fix_csl: cs.En.CSL,
        fix_init_csmseqdata: con.Pr.Iterable[Any],
        fix_manager: Optional[mp_mngr.SyncManager],
        fix_csmlseq_factory: Callable[[cs.En.CSL,
                                       con.Pr.Iterable[TYPE],
                                       Optional[mp_mngr.SyncManager]],
                                      csdata.Pr.CSMutableLengthSequence[TYPE]],
        fix_exec: execs.Im.Exec) -> None:

    fix_init_csmseqdata = tuple(fix_init_csmseqdata)
    init_len: Final[int] = len(fix_init_csmseqdata)
    assert init_len > 2

    csmlseq: csdata.Pr.CSMutableLengthSequence[Any] = cast(csdata.Pr.CSMutableLengthSequence[Any],
                                                           fix_csmlseq_factory(fix_csl,
                                                                               fix_init_csmseqdata,
                                                                               fix_manager))
    csmlseq_twin: List[Any] = list(fix_init_csmseqdata)

    # setitem - diff len
    for setitem_name in (csmlseq.__setitem__.__name__, csmlseq.c_set.__name__):
        # incr len
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': ft.c_get_and_apply,
                            'params_1': ((), {
                                'el': ft.c_identity,
                                'params': ((), {
                                    'el': csmlseq
                                }),
                                'apply_func': (type(csmlseq),
                                               setitem_name),
                                'apply_params': ((slice(1, 2), (1, "asdf", 242.54)), {})
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': ft.c_get_and_apply(el=ft.c_identity,
                                                         params=((csmlseq_twin,), {}),
                                                         apply_func=(type(csmlseq_twin),
                                                                     csmlseq_twin.__setitem__.__name__),
                                                         apply_params=(
                                                             (slice(1, 2), (1, "asdf", 242.54)), {}))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {
                                        'condition_check': ft.c_eq,
                                        'params': ((), {
                                            'el_1': (csmlseq,
                                                     csmlseq.c_len.__name__),
                                            'params_1': None,
                                            'el_2': ft.c_identity,
                                            'params_2': ((), {
                                                'el': len(fix_init_csmseqdata) + 2
                                            })
                                        }),
                                        'max_check_count': 1
                                    }),
                                    join=True)
            ],
            manager=fix_manager
        )

        csmlseq[0:] = fix_init_csmseqdata
        csmlseq_twin[0:] = fix_init_csmseqdata

        # setitem append
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': ft.c_get_and_apply,
                            'params_1': ((), {
                                'el': ft.c_identity,
                                'params': ((), {
                                    'el': csmlseq
                                }),
                                'apply_func': (type(csmlseq),
                                               setitem_name),
                                'apply_params': ((slice(init_len, init_len + 10), (1, "asdf", 242.54)), {})
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': ft.c_get_and_apply(
                                    el=ft.c_identity,
                                    params=((csmlseq_twin,), {}),
                                    apply_func=(type(csmlseq_twin),
                                                csmlseq_twin.__setitem__.__name__),
                                    apply_params=(
                                        (slice(init_len, init_len + 10), (1, "asdf", 242.54)), {}))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {
                                        'condition_check': ft.c_eq,
                                        'params': ((), {
                                            'el_1': (csmlseq,
                                                     csmlseq.c_len.__name__),
                                            'params_1': None,
                                            'el_2': ft.c_identity,
                                            'params_2': ((), {
                                                'el': len(fix_init_csmseqdata) + 3
                                            })
                                        }),
                                        'max_check_count': 1
                                    }),
                                    join=True)
            ],
            manager=fix_manager
        )

        csmlseq[0:] = fix_init_csmseqdata
        csmlseq_twin[0:] = fix_init_csmseqdata

        # setitem append 2
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': ft.c_get_and_apply,
                            'params_1': ((), {
                                'el': ft.c_identity,
                                'params': ((), {
                                    'el': csmlseq
                                }),
                                'apply_func': (type(csmlseq),
                                               setitem_name),
                                'apply_params': ((slice(init_len + 5, init_len + 10), (1, "asdf", 242.54)), {})
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': ft.c_get_and_apply(
                                    el=ft.c_identity,
                                    params=((csmlseq_twin,), {}),
                                    apply_func=(type(csmlseq_twin),
                                                csmlseq_twin.__setitem__.__name__),
                                    apply_params=(
                                        (slice(init_len + 5, init_len + 10), (1, "asdf", 242.54)), {}))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {
                                        'condition_check': ft.c_eq,
                                        'params': ((), {
                                            'el_1': (csmlseq,
                                                     csmlseq.c_len.__name__),
                                            'params_1': None,
                                            'el_2': ft.c_identity,
                                            'params_2': ((), {
                                                'el': len(fix_init_csmseqdata) + 3
                                            })
                                        }),
                                        'max_check_count': 1
                                    }),
                                    join=True)
            ],
            manager=fix_manager
        )

        csmlseq[0:] = fix_init_csmseqdata
        csmlseq_twin[0:] = fix_init_csmseqdata

        # setitem decr len
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': ft.c_get_and_apply,
                            'params_1': ((), {
                                'el': ft.c_identity,
                                'params': ((), {
                                    'el': csmlseq
                                }),
                                'apply_func': (type(csmlseq),
                                               setitem_name),
                                'apply_params': ((slice(0, 3), (1, "asdf")), {})
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': ft.c_get_and_apply(el=ft.c_identity,
                                                         params=((csmlseq_twin,), {}),
                                                         apply_func=(type(csmlseq_twin),
                                                                     csmlseq_twin.__setitem__.__name__),
                                                         apply_params=(
                                                             (slice(0, 3), (1, "asdf")), {}))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {
                                        'condition_check': ft.c_eq,
                                        'params': ((), {
                                            'el_1': (csmlseq,
                                                     csmlseq.c_len.__name__),
                                            'params_1': None,
                                            'el_2': ft.c_identity,
                                            'params_2': ((), {
                                                'el': len(fix_init_csmseqdata) - 1
                                            })
                                        }),
                                        'max_check_count': 1
                                    }),
                                    join=True)
            ],
            manager=fix_manager
        )
        csmlseq[0:] = fix_init_csmseqdata
        csmlseq_twin[0:] = fix_init_csmseqdata

    # delitem
    for delitem_name in (csmlseq.__delitem__.__name__, csmlseq.c_delete.__name__):
        # int index
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': ft.c_get_and_apply,
                            'params_1': ((), {
                                'el': ft.c_identity,
                                'params': ((), {
                                    'el': csmlseq
                                }),
                                'apply_func': (type(csmlseq),
                                               delitem_name),
                                'apply_params': ((2,), {})
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': ft.c_get_and_apply(el=ft.c_identity,
                                                         params=((csmlseq_twin,), {}),
                                                         apply_func=(type(csmlseq_twin),
                                                                     csmlseq_twin.__delitem__.__name__),
                                                         apply_params=(
                                                             (2,), {}))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {
                                        'condition_check': ft.c_eq,
                                        'params': ((), {
                                            'el_1': (csmlseq,
                                                     csmlseq.c_len.__name__),
                                            'params_1': None,
                                            'el_2': ft.c_identity,
                                            'params_2': ((), {
                                                'el': len(fix_init_csmseqdata) - 1
                                            })
                                        }),
                                        'max_check_count': 1
                                    }),
                                    join=True)
            ],
            manager=fix_manager
        )

        csmlseq[0:] = fix_init_csmseqdata
        csmlseq_twin[0:] = fix_init_csmseqdata

        # int IndexError
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmlseq,
                                       delitem_name),
                            'params': ((-(init_len + 1),), {}),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmlseq,
                                       delitem_name),
                            'params': ((-(init_len + 1000),), {}),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmlseq,
                                       delitem_name),
                            'params': ((init_len,), {}),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmlseq,
                                       delitem_name),
                            'params': ((init_len + 1000,), {}),
                            'expected_exception': IndexError
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                )
            ],
            manager=fix_manager
        )

        # slice index
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': ft.c_get_and_apply,
                            'params_1': ((), {
                                'el': ft.c_identity,
                                'params': ((), {
                                    'el': csmlseq
                                }),
                                'apply_func': (type(csmlseq),
                                               delitem_name),
                                'apply_params': ((slice(1, 3),), {})
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': ft.c_get_and_apply(el=ft.c_identity,
                                                         params=((csmlseq_twin,), {}),
                                                         apply_func=(type(csmlseq_twin),
                                                                     csmlseq_twin.__delitem__.__name__),
                                                         apply_params=(
                                                             (slice(1, 3),), {}))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {
                                        'condition_check': ft.c_eq,
                                        'params': ((), {
                                            'el_1': (csmlseq,
                                                     csmlseq.c_len.__name__),
                                            'params_1': None,
                                            'el_2': ft.c_identity,
                                            'params_2': ((), {
                                                'el': len(fix_init_csmseqdata) - 2
                                            })
                                        }),
                                        'max_check_count': 1
                                    }),
                                    join=True)
            ],
            manager=fix_manager
        )

        csmlseq[0:] = fix_init_csmseqdata
        csmlseq_twin[0:] = fix_init_csmseqdata

        # slice index
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': ft.c_get_and_apply,
                            'params_1': ((), {
                                'el': ft.c_identity,
                                'params': ((), {
                                    'el': csmlseq
                                }),
                                'apply_func': (type(csmlseq),
                                               delitem_name),
                                'apply_params': ((slice(init_len - 2, init_len + 100),), {})
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': ft.c_get_and_apply(el=ft.c_identity,
                                                         params=((csmlseq_twin,), {}),
                                                         apply_func=(type(csmlseq_twin),
                                                                     csmlseq_twin.__delitem__.__name__),
                                                         apply_params=(
                                                             (slice(init_len - 2, init_len + 100),), {}))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {
                                        'condition_check': ft.c_eq,
                                        'params': ((), {
                                            'el_1': (csmlseq,
                                                     csmlseq.c_len.__name__),
                                            'params_1': None,
                                            'el_2': ft.c_identity,
                                            'params_2': ((), {
                                                'el': len(fix_init_csmseqdata) - 2
                                            })
                                        }),
                                        'max_check_count': 1
                                    }),
                                    join=True)
            ],
            manager=fix_manager
        )

        csmlseq[0:] = fix_init_csmseqdata
        csmlseq_twin[0:] = fix_init_csmseqdata

        # slice index
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': ft.c_get_and_apply,
                            'params_1': ((), {
                                'el': ft.c_identity,
                                'params': ((), {
                                    'el': csmlseq
                                }),
                                'apply_func': (type(csmlseq),
                                               delitem_name),
                                'apply_params': ((slice(init_len, init_len + 100),), {})
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': ft.c_get_and_apply(el=ft.c_identity,
                                                         params=((csmlseq_twin,), {}),
                                                         apply_func=(type(csmlseq_twin),
                                                                     csmlseq_twin.__delitem__.__name__),
                                                         apply_params=(
                                                             (slice(init_len, init_len + 100),), {}))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {
                                        'condition_check': ft.c_eq,
                                        'params': ((), {
                                            'el_1': (csmlseq,
                                                     csmlseq.c_len.__name__),
                                            'params_1': None,
                                            'el_2': ft.c_identity,
                                            'params_2': ((), {
                                                'el': len(fix_init_csmseqdata)
                                            })
                                        }),
                                        'max_check_count': 1
                                    }),
                                    join=True)
            ],
            manager=fix_manager
        )

    # iadd, extend, c_extend
    for extend_name in (csmlseq.__iadd__.__name__, csmlseq.extend.__name__, csmlseq.c_extend.__name__):
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': ft.c_get_and_apply,
                            'params_1': ((), {
                                'el': ft.c_identity,
                                'params': ((), {
                                    'el': csmlseq
                                }),
                                'apply_func': (type(csmlseq),
                                               extend_name),
                                'apply_params': (((2, "asb", 3.5),), {})
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': ft.c_get_and_apply(el=ft.c_identity,
                                                         params=((csmlseq_twin,), {}),
                                                         apply_func=(type(csmlseq_twin),
                                                                     csmlseq_twin.extend.__name__),
                                                         apply_params=(
                                                             ((2, "asb", 3.5),), {}))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {
                                        'condition_check': ft.c_eq,
                                        'params': ((), {
                                            'el_1': (csmlseq,
                                                     csmlseq.c_len.__name__),
                                            'params_1': None,
                                            'el_2': ft.c_identity,
                                            'params_2': ((), {
                                                'el': len(fix_init_csmseqdata) + 3
                                            })
                                        }),
                                        'max_check_count': 1
                                    }),
                                    join=True)
            ],
            manager=fix_manager
        )

        csmlseq[0:] = fix_init_csmseqdata
        csmlseq_twin[0:] = fix_init_csmseqdata

    # append, c_append
    for append_name in (csmlseq.append.__name__, csmlseq.c_append.__name__):
        fix_exec.c_exec_multiple(
            csl=fix_csl,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_elem_wise_eq,
                        'params': ((), {
                            'el_1': ft.c_get_and_apply,
                            'params_1': ((), {
                                'el': ft.c_identity,
                                'params': ((), {
                                    'el': csmlseq
                                }),
                                'apply_func': (type(csmlseq),
                                               append_name),
                                'apply_params': (("asb",), {})
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((), {
                                'el': ft.c_get_and_apply(el=ft.c_identity,
                                                         params=((csmlseq_twin,), {}),
                                                         apply_func=(type(csmlseq_twin),
                                                                     csmlseq_twin.append.__name__),
                                                         apply_params=(
                                                             ("asb",), {}))
                            })
                        }),
                        'max_check_count': 1
                    }),
                    join=True
                ),
                execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                    params=((), {
                                        'condition_check': ft.c_eq,
                                        'params': ((), {
                                            'el_1': (csmlseq,
                                                     csmlseq.c_len.__name__),
                                            'params_1': None,
                                            'el_2': ft.c_identity,
                                            'params_2': ((), {
                                                'el': len(fix_init_csmseqdata) + 1
                                            })
                                        }),
                                        'max_check_count': 1
                                    }),
                                    join=True)
            ],
            manager=fix_manager
        )

        csmlseq[0:] = fix_init_csmseqdata
        csmlseq_twin[0:] = fix_init_csmseqdata

    # clear
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': ft.c_get_and_apply,
                        'params_1': ((), {
                            'el': ft.c_identity,
                            'params': ((), {
                                'el': csmlseq
                            }),
                            'apply_func': (type(csmlseq),
                                           csmlseq.clear.__name__),
                            'apply_params': None
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': ft.c_get_and_apply(el=ft.c_identity,
                                                     params=((csmlseq_twin,), {}),
                                                     apply_func=(type(csmlseq_twin),
                                                                 csmlseq_twin.clear.__name__),
                                                     apply_params=None)
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': 0
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    # pop
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': ft.c_get_and_apply,
                        'params_1': ((), {
                            'el': ft.c_identity,
                            'params': ((), {
                                'el': csmlseq
                            }),
                            'apply_func': (type(csmlseq),
                                           csmlseq.pop.__name__),
                            'apply_params': None
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': ft.c_get_and_apply(el=ft.c_identity,
                                                     params=((csmlseq_twin,), {}),
                                                     apply_func=(type(csmlseq_twin),
                                                                 csmlseq_twin.pop.__name__),
                                                     apply_params=None)
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': len(fix_init_csmseqdata) - 1
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    # pop - index
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': ft.c_get_and_apply,
                        'params_1': ((), {
                            'el': ft.c_identity,
                            'params': ((), {
                                'el': csmlseq
                            }),
                            'apply_func': (type(csmlseq),
                                           csmlseq.pop.__name__),
                            'apply_params': ((2,), {})
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': ft.c_get_and_apply(el=ft.c_identity,
                                                     params=((csmlseq_twin,), {}),
                                                     apply_func=(type(csmlseq_twin),
                                                                 csmlseq_twin.pop.__name__),
                                                     apply_params=(
                                                         (2,), {}))
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': len(fix_init_csmseqdata) - 1
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    # pop - index
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_eq,
                    'params': ((), {
                        'el_1': (csmlseq,
                                 csmlseq.pop.__name__),
                        'params_1': ((2,), {}),
                        'el_2': (csmlseq_twin,
                                 csmlseq_twin.pop.__name__),
                        'params_2': ((2,), {})
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': len(fix_init_csmseqdata) - 1
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    # remove
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': ft.c_get_and_apply,
                        'params_1': ((), {
                            'el': ft.c_identity,
                            'params': ((), {
                                'el': csmlseq
                            }),
                            'apply_func': (type(csmlseq),
                                           csmlseq.remove.__name__),
                            'apply_params': ((csmlseq[1],), {})
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': ft.c_get_and_apply(el=ft.c_identity,
                                                     params=((csmlseq_twin,), {}),
                                                     apply_func=(type(csmlseq_twin),
                                                                 csmlseq_twin.remove.__name__),
                                                     apply_params=(
                                                         (csmlseq_twin[1],), {}))
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': len(fix_init_csmseqdata) - 1  # 'remove' removes first occurrence
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    # insert
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': ft.c_get_and_apply,
                        'params_1': ((), {
                            'el': ft.c_identity,
                            'params': ((), {
                                'el': csmlseq
                            }),
                            'apply_func': (type(csmlseq),
                                           csmlseq.insert.__name__),
                            'apply_params': ((1, "sadfasfd",), {})
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': ft.c_get_and_apply(el=ft.c_identity,
                                                     params=((csmlseq_twin,), {}),
                                                     apply_func=(type(csmlseq_twin),
                                                                 csmlseq_twin.insert.__name__),
                                                     apply_params=(
                                                         (1, "sadfasfd",), {}))
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': len(fix_init_csmseqdata) + 1
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    # inser at end
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': ft.c_get_and_apply,
                        'params_1': ((), {
                            'el': ft.c_identity,
                            'params': ((), {
                                'el': csmlseq
                            }),
                            'apply_func': (type(csmlseq),
                                           csmlseq.insert.__name__),
                            'apply_params': ((init_len + 1000, "sadfasfd",), {})
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': ft.c_get_and_apply(el=ft.c_identity,
                                                     params=((csmlseq_twin,), {}),
                                                     apply_func=(type(csmlseq_twin),
                                                                 csmlseq_twin.insert.__name__),
                                                     apply_params=(
                                                         (init_len + 123124, "sadfasfd",), {}))
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': len(fix_init_csmseqdata) + 1
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    # c_insert
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': ft.c_get_and_apply,
                        'params_1': ((), {
                            'el': ft.c_identity,
                            'params': ((), {
                                'el': csmlseq
                            }),
                            'apply_func': (type(csmlseq),
                                           csmlseq.c_insert.__name__),
                            'apply_params': ((1, ("sadfasfd", 3.4, 11),), {})
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': ft.c_get_and_apply(el=ft.c_identity,
                                                     params=((((csmlseq_twin[0],)
                                                               + cast(Tuple[Any, ...], ("sadfasfd", 3.4, 11))
                                                               + tuple(csmlseq_twin[1:])),), {}))
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': len(fix_init_csmseqdata) + 3
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    # c_insert at end
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': ft.c_get_and_apply,
                        'params_1': ((), {
                            'el': ft.c_identity,
                            'params': ((), {
                                'el': csmlseq
                            }),
                            'apply_func': (type(csmlseq),
                                           csmlseq.c_insert.__name__),
                            'apply_params': ((init_len + 12312, ("sadfasfd", 3.4, 11),), {})
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': ft.c_get_and_apply(el=ft.c_identity,
                                                     params=(((tuple(csmlseq_twin)
                                                               + cast(Tuple[Any, ...], ("sadfasfd", 3.4, 11))),), {}))
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': len(fix_init_csmseqdata) + 3
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    # apply - incr len
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': ft.c_get_and_apply,
                        'params_1': ((), {
                            'el': ft.c_identity,
                            'params': ((), {
                                'el': csmlseq
                            }),
                            'apply_func': (type(csmlseq),
                                           csmlseq.c_apply.__name__),
                            'apply_params': (None, {
                                'index': slice(1, 3),
                                'func': __apply_to_str_seq_incr_len
                            }),
                            'apply_check_result': (APPLY_RETURN,)  # Must be a tuple of length 1!
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': ft.c_get_and_apply(el=ft.c_identity,
                                                     params=((csmlseq_twin,), {}),
                                                     apply_func=__apply_helper,
                                                     apply_params=(None, {
                                                         'index': slice(1, 3),
                                                         'func': __apply_to_str_seq_incr_len
                                                     }),
                                                     apply_check_result=(APPLY_RETURN,))
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': len(fix_init_csmseqdata) + 1
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    # apply - decr len
    fix_exec.c_exec_multiple(
        csl=fix_csl,
        join=False,
        exec_params=[
            execs.Im.ExecParams(
                func_or_obj_func=ft.c_poll_condition,
                params=((), {
                    'condition_check': ft.c_elem_wise_eq,
                    'params': ((), {
                        'el_1': ft.c_get_and_apply,
                        'params_1': ((), {
                            'el': ft.c_identity,
                            'params': ((), {
                                'el': csmlseq
                            }),
                            'apply_func': (type(csmlseq),
                                           csmlseq.c_apply.__name__),
                            'apply_params': (None, {
                                'index': slice(1, 3),
                                'func': __apply_to_str_seq_decr_len
                            }),
                            'apply_check_result': (APPLY_RETURN,)  # Must be a tuple of length 1!
                        }),
                        'el_2': ft.c_identity,
                        'params_2': ((), {
                            'el': ft.c_get_and_apply(el=ft.c_identity,
                                                     params=((csmlseq_twin,), {}),
                                                     apply_func=__apply_helper,
                                                     apply_params=(None, {
                                                         'index': slice(1, 3),
                                                         'func': __apply_to_str_seq_decr_len
                                                     }),
                                                     apply_check_result=(APPLY_RETURN,))
                        })
                    }),
                    'max_check_count': 1
                }),
                join=True
            ),
            execs.Im.ExecParams(func_or_obj_func=ft.c_poll_condition,
                                params=((), {
                                    'condition_check': ft.c_eq,
                                    'params': ((), {
                                        'el_1': (csmlseq,
                                                 csmlseq.c_len.__name__),
                                        'params_1': None,
                                        'el_2': ft.c_identity,
                                        'params_2': ((), {
                                            'el': len(fix_init_csmseqdata) - 1
                                        })
                                    }),
                                    'max_check_count': 1
                                }),
                                join=True)
        ],
        manager=fix_manager
    )

    csmlseq[0:] = fix_init_csmseqdata
    csmlseq_twin[0:] = fix_init_csmseqdata

    fix_exec.c_join()


# noinspection PyArgumentList
def test_csmlseqdata_concurrent(
        fix_csl: cs.En.CSL,
        fix_init_csmseqdata: con.Pr.Iterable[Any],
        fix_manager: Optional[mp_mngr.SyncManager],
        fix_csmlseq_factory: Callable[[cs.En.CSL,
                                       con.Pr.Iterable[TYPE],
                                       Optional[mp_mngr.SyncManager]],
                                      csdata.Pr.CSMutableLengthSequence[TYPE]],
        fix_exec: execs.Im.Exec) -> None:

    fix_init_csmseqdata = tuple(fix_init_csmseqdata)
    init_len: Final[int] = len(fix_init_csmseqdata)
    assert init_len > 2

    csmlseq: csdata.Pr.CSMutableLengthSequence[Any] = cast(csdata.Pr.CSMutableLengthSequence[Any],
                                                           fix_csmlseq_factory(fix_csl,
                                                                               fix_init_csmseqdata,
                                                                               fix_manager))

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        # test_blocking - delete
        for del_name in (csmlseq.__delitem__.__name__, csmlseq.c_delete.__name__):
            # int index
            execs.Ca.c_exec_multiple(
                csl=fix_csl,
                exec_=fix_exec,
                join=False,
                exec_params=[
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_eq,
                            'params': ((), {
                                'el_1': (csmlseq,
                                         csmlseq.c_apply.__name__),
                                'params_1': ((), {
                                    'index': 0,
                                    'func': __apply_delay
                                }),
                                'el_2': ft.c_identity,
                                'params_2': ((APPLY_RETURN,), None)
                            }),
                            'max_check_count': 1
                        }),
                        join=False
                    ),
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_raises,
                            'params': ((), {
                                'raiser': (csmlseq,
                                           csmlseq.c_apply.__name__),
                                'params': ((), {
                                    'index': 0,
                                    'func': __apply_no_delay,
                                    'blocking': False
                                }),
                                'expected_exception': TimeoutError
                            })
                        }),
                        join=True
                    ),
                    execs.Im.ExecDelayedParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_eq,
                            'params': ((), {
                                'el_1': (csmlseq,
                                         del_name),
                                'params_1': ((1,), {}),
                                'el_2': ft.c_identity,
                                'params_2': ((None,), None)
                            }),
                            'max_check_count': 1
                        }),
                        join=True,
                        min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                        max_sec=APPLY_TIMEOUT_SHORT + 1.0
                    )
                ],
                manager=fix_manager,
                _min_sec=APPLY_TIMEOUT_SHORT,
                _max_sec=APPLY_TIMEOUT_SHORT + 1.0
            )

            csmlseq[0:] = fix_init_csmseqdata

            # slice index
            execs.Ca.c_exec_multiple(
                csl=fix_csl,
                exec_=fix_exec,
                join=False,
                exec_params=[
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_eq,
                            'params': ((), {
                                'el_1': (csmlseq,
                                         csmlseq.c_apply.__name__),
                                'params_1': ((), {
                                    'index': 0,
                                    'func': __apply_delay
                                }),
                                'el_2': ft.c_identity,
                                'params_2': ((APPLY_RETURN,), None)
                            }),
                            'max_check_count': 1
                        }),
                        join=False
                    ),
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_raises,
                            'params': ((), {
                                'raiser': (csmlseq,
                                           csmlseq.c_apply.__name__),
                                'params': ((), {
                                    'index': 0,
                                    'func': __apply_no_delay,
                                    'blocking': False
                                }),
                                'expected_exception': TimeoutError
                            })
                        }),
                        join=True
                    ),
                    execs.Im.ExecDelayedParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_eq,
                            'params': ((), {
                                'el_1': (csmlseq,
                                         del_name),
                                'params_1': ((slice(1, 3),), {}),
                                'el_2': ft.c_identity,
                                'params_2': ((None,), None)
                            }),
                            'max_check_count': 1
                        }),
                        join=True,
                        min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                        max_sec=APPLY_TIMEOUT_SHORT + 1.0
                    )
                ],
                manager=fix_manager,
                _min_sec=APPLY_TIMEOUT_SHORT,
                _max_sec=APPLY_TIMEOUT_SHORT + 1.0
            )
            csmlseq[0:] = fix_init_csmseqdata

        # test blocking - extend, iadd, c_extend
        for extend_name in (csmlseq.__iadd__.__name__, csmlseq.extend.__name__, csmlseq.c_extend.__name__):
            execs.Ca.c_exec_multiple(
                csl=fix_csl,
                exec_=fix_exec,
                join=False,
                exec_params=[
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_eq,
                            'params': ((), {
                                'el_1': (csmlseq,
                                         csmlseq.c_apply.__name__),
                                'params_1': ((), {
                                    'index': 0,
                                    'func': __apply_delay
                                }),
                                'el_2': ft.c_identity,
                                'params_2': ((APPLY_RETURN,), None)
                            }),
                            'max_check_count': 1
                        }),
                        join=False
                    ),
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_raises,
                            'params': ((), {
                                'raiser': (csmlseq,
                                           csmlseq.c_apply.__name__),
                                'params': ((), {
                                    'index': 0,
                                    'func': __apply_no_delay,
                                    'blocking': False
                                }),
                                'expected_exception': TimeoutError
                            })
                        }),
                        join=True
                    ),
                    execs.Im.ExecDelayedParams(
                        func_or_obj_func=(csmlseq,
                                          extend_name),
                        params=(((1, 2, "sadf"),), {}),
                        join=True,
                        min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                        max_sec=APPLY_TIMEOUT_SHORT + 1.0
                    )
                ],
                manager=fix_manager,
                _min_sec=APPLY_TIMEOUT_SHORT,
                _max_sec=APPLY_TIMEOUT_SHORT + 1.0
            )
            csmlseq[0:] = fix_init_csmseqdata

        # test blocking - append, c_append
        for append_name in (csmlseq.append.__name__, csmlseq.c_append.__name__):
            execs.Ca.c_exec_multiple(
                csl=fix_csl,
                exec_=fix_exec,
                join=False,
                exec_params=[
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_eq,
                            'params': ((), {
                                'el_1': (csmlseq,
                                         csmlseq.c_apply.__name__),
                                'params_1': ((), {
                                    'index': 0,
                                    'func': __apply_delay
                                }),
                                'el_2': ft.c_identity,
                                'params_2': ((APPLY_RETURN,), None)
                            }),
                            'max_check_count': 1
                        }),
                        join=False
                    ),
                    execs.Im.ExecParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_raises,
                            'params': ((), {
                                'raiser': (csmlseq,
                                           csmlseq.c_apply.__name__),
                                'params': ((), {
                                    'index': 0,
                                    'func': __apply_no_delay,
                                    'blocking': False
                                }),
                                'expected_exception': TimeoutError
                            })
                        }),
                        join=True
                    ),
                    execs.Im.ExecDelayedParams(
                        func_or_obj_func=ft.c_poll_condition,
                        params=((), {
                            'condition_check': ft.c_eq,
                            'params': ((), {
                                'el_1': (csmlseq,
                                         append_name),
                                'params_1': ((3342,), {}),
                                'el_2': ft.c_identity,
                                'params_2': ((None,), None)
                            }),
                            'max_check_count': 1
                        }),
                        join=True,
                        min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                        max_sec=APPLY_TIMEOUT_SHORT + 1.0
                    )
                ],
                manager=fix_manager,
                _min_sec=APPLY_TIMEOUT_SHORT,
                _max_sec=APPLY_TIMEOUT_SHORT + 1.0
            )
            csmlseq[0:] = fix_init_csmseqdata

        # test blocking - clear
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmlseq,
                                     csmlseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmlseq,
                                       csmlseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmlseq,
                                     csmlseq.clear.__name__),
                            'params_1': ((), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((None,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )
        csmlseq[0:] = fix_init_csmseqdata

        # test blocking - insert
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmlseq,
                                     csmlseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmlseq,
                                       csmlseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmlseq,
                                     csmlseq.insert.__name__),
                            'params_1': ((1, "sadf"), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((None,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )
        csmlseq[0:] = fix_init_csmseqdata

        # test blocking - pop
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmlseq,
                                     csmlseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmlseq,
                                       csmlseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmlseq,
                                     csmlseq.pop.__name__),
                            'params_1': ((), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((fix_init_csmseqdata[-1],), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )
        csmlseq[0:] = fix_init_csmseqdata

        # test blocking - remove
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmlseq,
                                     csmlseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmlseq,
                                       csmlseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmlseq,
                                     csmlseq.remove.__name__),
                            'params_1': ((csmlseq[1],), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((None,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )
        csmlseq[0:] = fix_init_csmseqdata

        # test blocking - remove
        execs.Ca.c_exec_multiple(
            csl=fix_csl,
            exec_=fix_exec,
            join=False,
            exec_params=[
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmlseq,
                                     csmlseq.c_apply.__name__),
                            'params_1': ((), {
                                'index': 0,
                                'func': __apply_delay
                            }),
                            'el_2': ft.c_identity,
                            'params_2': ((APPLY_RETURN,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=False
                ),
                execs.Im.ExecParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_raises,
                        'params': ((), {
                            'raiser': (csmlseq,
                                       csmlseq.c_apply.__name__),
                            'params': ((), {
                                'index': 0,
                                'func': __apply_no_delay,
                                'blocking': False
                            }),
                            'expected_exception': TimeoutError
                        })
                    }),
                    join=True
                ),
                execs.Im.ExecDelayedParams(
                    func_or_obj_func=ft.c_poll_condition,
                    params=((), {
                        'condition_check': ft.c_eq,
                        'params': ((), {
                            'el_1': (csmlseq,
                                     csmlseq.c_insert.__name__),
                            'params_1': ((1, (2, 4, "sdf")), {}),
                            'el_2': ft.c_identity,
                            'params_2': ((None,), None)
                        }),
                        'max_check_count': 1
                    }),
                    join=True,
                    min_sec=APPLY_TIMEOUT_SHORT - 2.0,
                    max_sec=APPLY_TIMEOUT_SHORT + 1.0
                )
            ],
            manager=fix_manager,
            _min_sec=APPLY_TIMEOUT_SHORT,
            _max_sec=APPLY_TIMEOUT_SHORT + 1.0
        )
        csmlseq[0:] = fix_init_csmseqdata

    fix_exec.c_join()


QUEUE_CAP: Final[int] = 10
FILL_ELEM: Final[str] = "ABCDEFGASDASDLSLSLSLSLALAEIOIQEGBASKFJBAKDLGJBAKDJBFAKLSD"


def __c_fill_capqueue(queue: csdata.Pr.CSCapacityQueue[Any],
                      full_timeout: Optional[float] = None) -> None:
    assert queue.c_capacity() == QUEUE_CAP
    assert queue.qsize() <= QUEUE_CAP

    for _ in range(QUEUE_CAP - queue.qsize()):
        queue.c_put(item=FILL_ELEM)

    assert queue.qsize() == QUEUE_CAP

    with pytest.raises(Full):
        queue.c_put(item=FILL_ELEM,
                    blocking=False)

    if full_timeout is not None:
        with pytest.raises(Full):
            queue.c_put(item=FILL_ELEM,
                        blocking=True,
                        timeout=full_timeout)


def __c_empty_capqueue(queue: csdata.Pr.CSCapacityQueue[TYPE],
                       empty_timeout: Optional[float] = None) -> None:
    assert queue.c_capacity() == QUEUE_CAP
    assert queue.qsize() <= QUEUE_CAP

    for _ in range(queue.qsize()):
        queue.c_get()

    assert queue.qsize() == 0

    with pytest.raises(Empty):
        queue.c_get(blocking=False)

    if empty_timeout is not None:
        with pytest.raises(Empty):
            queue.c_get(blocking=True,
                        timeout=empty_timeout)


def __c_put_all_in_capqueue(queue: csdata.Pr.CSCapacityQueue[Any],
                            items: con.Pr.Sequence[Any]) -> None:
    assert c_assert_not_none(queue.c_capacity()) - queue.qsize() >= len(items)
    for item in items:
        queue.c_put(item=item)


def __c_compare_all_in_capqueue(queue: csdata.Pr.CSCapacityQueue[Any],
                                items: con.Pr.Sequence[Any]) -> None:
    assert queue.qsize() == len(items)
    for i in range(queue.qsize()):
        assert items[i] == queue.c_get()

    assert queue.empty()


# noinspection PyArgumentList
def test_cscapacityqueue(fix_csl: cs.En.CSL,
                         fix_init_queue: con.Pr.SizedIterable[Any],
                         fix_manager: Optional[mp_mngr.SyncManager],
                         fix_cscapacityqueue_factory: Callable[[cs.En.CSL,
                                                                con.Pr.SizedIterable[TYPE],
                                                                Optional[int],
                                                                Optional[mp_mngr.SyncManager]],
                                                               csdata.Pr.CSCapacityQueue[TYPE]],
                         fix_exec: execs.Im.Exec) -> None:

    fix_init_queue = con.Ca.c_to_seq(obj=fix_init_queue)

    assert len(fix_init_queue) <= QUEUE_CAP

    cs_cap_queue: Final[csdata.Pr.CSCapacityQueue[Any]] = fix_cscapacityqueue_factory(
        fix_csl,
        fix_init_queue,
        QUEUE_CAP,
        fix_manager
    )

    execs.Ca.c_exec_multiple(csl=fix_csl,
                             exec_=fix_exec,
                             join=False,
                             exec_params=[
                                 execs.Im.ExecParams(
                                     func_or_obj_func=ft.c_poll_condition,
                                     params=((), {
                                         'condition_check': ft.c_eq,
                                         'params': ((), {
                                             'el_1': (cs_cap_queue,
                                                      cs_cap_queue.qsize.__name__),
                                             'params_1': None,
                                             'el_2': ft.c_identity,
                                             'params_2': ((), {'el': len(fix_init_queue)})
                                         }),
                                         'max_check_count': 1
                                     }),
                                     join=True),
                                 execs.Im.ExecDelayedParams(
                                     func_or_obj_func=__c_fill_capqueue,
                                     params=((), {
                                         'queue': cs_cap_queue,
                                         'full_timeout': 4.0
                                     }),
                                     join=True,
                                     min_sec=4.0,
                                     max_sec=7.0)
                             ],
                             manager=fix_manager,
                             _min_sec=4.0,
                             _max_sec=7.0)

    assert cs_cap_queue.full()
    assert cs_cap_queue.qsize() == QUEUE_CAP == cs_cap_queue.c_capacity()

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        execs.Ca.c_exec_multiple(csl=fix_csl,
                                 exec_=fix_exec,
                                 join=False,
                                 exec_params=[
                                     execs.Im.ExecDelayedParams(
                                         func_or_obj_func=(cs_cap_queue,
                                                           cs_cap_queue.c_get.__name__),
                                         params=None,
                                         join=False,
                                         in_delay=4.0),
                                     execs.Im.ExecDelayedParams(
                                         func_or_obj_func=(cs_cap_queue,
                                                           cs_cap_queue.c_put.__name__),
                                         params=((), {
                                             'item': FILL_ELEM
                                         }),
                                         join=True,
                                         min_sec=3.0,
                                         max_sec=5.0)
                                 ],
                                 manager=fix_manager,
                                 _min_sec=3.0,
                                 _max_sec=5.0)

        assert cs_cap_queue.full()
        assert cs_cap_queue.qsize() == QUEUE_CAP == cs_cap_queue.c_capacity()

    execs.Ca.c_exec_multiple(csl=fix_csl,
                             exec_=fix_exec,
                             join=False,
                             exec_params=[
                                 execs.Im.ExecDelayedParams(
                                     func_or_obj_func=__c_empty_capqueue,
                                     params=((), {
                                         'queue': cs_cap_queue,
                                         'empty_timeout': 4.0
                                     }),
                                     join=True,
                                     min_sec=4.0,
                                     max_sec=7.0)
                             ],
                             manager=fix_manager,
                             _min_sec=4.0,
                             _max_sec=7.0)

    assert cs_cap_queue.empty()
    assert cs_cap_queue.qsize() == 0

    if fix_csl > cs.En.CSL.SINGLE_THREAD:
        execs.Ca.c_exec_multiple(csl=fix_csl,
                                 exec_=fix_exec,
                                 join=False,
                                 exec_params=[
                                     execs.Im.ExecDelayedParams(
                                         func_or_obj_func=(cs_cap_queue,
                                                           cs_cap_queue.c_put.__name__),
                                         params=((), {
                                             'item': FILL_ELEM
                                         }),
                                         join=False,
                                         in_delay=4.0),
                                     execs.Im.ExecDelayedParams(
                                         func_or_obj_func=ft.c_poll_condition,
                                         params=((), {
                                             'condition_check': ft.c_eq,
                                             'params': ((), {
                                                 'el_1': (cs_cap_queue,
                                                          cs_cap_queue.c_get.__name__),
                                                 'params_1': ((), {}),
                                                 'el_2': ft.c_identity,
                                                 'params_2': ((), {
                                                     'el': FILL_ELEM
                                                 })
                                             }),
                                             'max_check_count': 1
                                         }),
                                         join=True,
                                         min_sec=3.0,
                                         max_sec=5.0)
                                 ],
                                 manager=fix_manager,
                                 _min_sec=3.0,
                                 _max_sec=5.0)

        assert cs_cap_queue.empty()
        assert cs_cap_queue.qsize() == 0

    execs.Ca.c_exec_multiple(csl=fix_csl,
                             exec_=fix_exec,
                             join=False,
                             exec_params=[
                                 execs.Im.ExecParams(
                                     func_or_obj_func=__c_put_all_in_capqueue,
                                     params=((), {
                                         'queue': cs_cap_queue,
                                         'items': fix_init_queue
                                     }),
                                     join=True),
                                 execs.Im.ExecParams(
                                     func_or_obj_func=ft.c_poll_condition,
                                     params=((), {
                                         'condition_check': ft.c_eq,
                                         'params': ((), {
                                             'el_1': (cs_cap_queue,
                                                      cs_cap_queue.qsize.__name__),
                                             'params_1': None,
                                             'el_2': ft.c_identity,
                                             'params_2': ((), {'el': len(fix_init_queue)})
                                         }),
                                         'max_check_count': 1
                                     }),
                                     join=True),
                                 execs.Im.ExecParams(
                                     func_or_obj_func=__c_compare_all_in_capqueue,
                                     params=((), {
                                         'queue': cs_cap_queue,
                                         'items': fix_init_queue
                                     }),
                                     join=True),
                             ],
                             manager=fix_manager)

    fix_exec.c_join()


# noinspection PyArgumentList
def test_cschunkcapacityqueue(
        fix_csl: cs.En.CSL,
        fix_manager: Optional[mp_mngr.SyncManager],
        fix_cschunkcapacityqueue_factory: Callable[[cs.En.CSL,
                                                    con.Pr.SizedIterable[Any],
                                                    Optional[int],
                                                    Optional[int],
                                                    Optional[int],
                                                    Optional[mp_mngr.SyncManager]],
                                                   csdata.Pr.CSCapacityQueue[Any]],
        fix_exec: execs.Im.Exec) -> None:

    cs_cap_queue: csdata.Pr.CSCapacityQueue[Any]

    with pytest.raises(ValueError):
        fix_cschunkcapacityqueue_factory(
            fix_csl,
            (),
            QUEUE_CAP,
            -1,
            None,
            fix_manager)

    with pytest.raises(ValueError):
        fix_cschunkcapacityqueue_factory(
            fix_csl,
            (),
            QUEUE_CAP,
            0,
            -22,
            fix_manager)

    with pytest.raises(ValueError):
        fix_cschunkcapacityqueue_factory(
            fix_csl,
            (),
            QUEUE_CAP,
            22,
            21,
            fix_manager)

    cs_cap_queue = fix_cschunkcapacityqueue_factory(
        fix_csl,
        cast(con.Pr.SizedIterable[Any], ()),
        QUEUE_CAP,
        4,
        4,
        fix_manager)

    with pytest.raises(ValueError):
        cs_cap_queue.c_put(item=(3, 4, 5))

    with pytest.raises(ValueError):
        cs_cap_queue.put((3, 4, 5))

    with pytest.raises(ValueError):
        cs_cap_queue.put_nowait((3, 4, 5))

    with pytest.raises(ValueError):
        cs_cap_queue.c_put(item=(3, 4, 5, 5, 6))

    with pytest.raises(ValueError):
        cs_cap_queue.put((3, 4, 5, 5, 6))

    with pytest.raises(ValueError):
        cs_cap_queue.put_nowait((3, 4, 5, 5, 6))

    cs_cap_queue.c_put((3, 4, 5, 5))
    execs.Ca.c_exec_multiple(csl=fix_csl,
                             exec_=fix_exec,
                             join=False,
                             exec_params=[
                                 execs.Im.ExecParams(
                                     func_or_obj_func=ft.c_poll_condition,
                                     params=((), {
                                         'condition_check': ft.c_eq,
                                         'params': ((), {
                                             'el_1': (cs_cap_queue,
                                                      cs_cap_queue.c_get.__name__),
                                             'params_1': ((), {}),
                                             'el_2': ft.c_identity,
                                             'params_2': ((), {
                                                 'el': (3, 4, 5, 5)
                                             })
                                         }),
                                         'max_check_count': 1
                                     }),
                                     join=True)
                             ],
                             manager=fix_manager)

    cs_cap_queue = fix_cschunkcapacityqueue_factory(
        fix_csl,
        (),
        QUEUE_CAP,
        4,
        6,
        fix_manager)

    with pytest.raises(ValueError):
        cs_cap_queue.c_put(item=(3, 4, 5))

    with pytest.raises(ValueError):
        cs_cap_queue.put((3, 4, 5))

    with pytest.raises(ValueError):
        cs_cap_queue.put_nowait((3, 4, 5))

    with pytest.raises(ValueError):
        cs_cap_queue.c_put(item=(3, 4, 5, 5, 6, 5, 6))

    with pytest.raises(ValueError):
        cs_cap_queue.put((3, 4, 5, 5, 6, 5, 6))

    with pytest.raises(ValueError):
        cs_cap_queue.put_nowait((3, 4, 5, 5, 6, 5, 6))

    cs_cap_queue.c_put((3, 4, 5, 5))
    execs.Ca.c_exec_multiple(csl=fix_csl,
                             exec_=fix_exec,
                             join=False,
                             exec_params=[
                                 execs.Im.ExecParams(
                                     func_or_obj_func=ft.c_poll_condition,
                                     params=((), {
                                         'condition_check': ft.c_eq,
                                         'params': ((), {
                                             'el_1': (cs_cap_queue,
                                                      cs_cap_queue.c_get.__name__),
                                             'params_1': ((), {}),
                                             'el_2': ft.c_identity,
                                             'params_2': ((), {
                                                 'el': (3, 4, 5, 5)
                                             })
                                         }),
                                         'max_check_count': 1
                                     }),
                                     join=True)
                             ],
                             manager=fix_manager)

    fix_exec.c_join()
