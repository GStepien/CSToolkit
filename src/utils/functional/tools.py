import time
from typing import Optional, Tuple, Any, Callable, Final, Dict, Union, Type, cast
import utils.types.containers as con
from utils.types.casts import c_assert_not_none
from utils.types.typevars import TYPE
from concurrency import cslocks


def c_normalize_params(params: Optional[Tuple[Optional[con.Pr.Iterable[Any]],
                                              Optional[con.Pr.Mapping[str, Any]]]]) -> Tuple[Tuple[Any, ...],
                                                                                             Dict[str, Any]]:
    if params is None:
        return tuple(), dict()
    else:
        return ((tuple()
                 if params[0] is None
                 else tuple(params[0])),
                (dict()
                 if params[1] is None
                 else dict(params[1])))


def c_normalize_func(func_or_obj_func: Union[Callable[..., Any], Tuple[Any, str]]) -> Callable[..., Any]:
    if callable(func_or_obj_func):
        return func_or_obj_func
    else:
        result: Any = getattr(func_or_obj_func[0], func_or_obj_func[1])
        if not callable(result):
            raise ValueError(f"Retrieved field from 'func_or_obj_func[0]' is not callable.")
        else:
            return cast(Callable[..., Any], result)


def __c_normalize_condition_check(condition_check: Union[Callable[..., bool], Tuple[Any, str]]) \
        -> Callable[..., bool]:
    return cast(Callable[..., bool], c_normalize_func(func_or_obj_func=condition_check))


def c_poll_condition(condition_check: Union[Callable[..., bool], Tuple[Any, str]],
                     params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]],
                     poll_interval: float = cslocks.Co.f_POLLING_TIMEOUT,
                     max_duration: Optional[float] = None,
                     max_check_count: Optional[int] = None) -> None:
    """
    'condition_check' is either a callable or a tuple consisting of an object and a function name to
    call on said object.
    Note: At least one check is performed even if max_time is <= 0.

    IMPORTANT: DO not set poll_interval too small in a concurrent locking scenario as this might lead to
    starvation!
    """

    poll_interval = max(0.0, poll_interval)

    args, kwargs = c_normalize_params(params=params)
    condition_check = __c_normalize_condition_check(condition_check=condition_check)

    if max_check_count is not None and max_check_count < 1:
        raise ValueError(f"'max_check_count' must be None or >= 1 (is {max_check_count}).")

    start_time: Final[Optional[float]] = (time.perf_counter()
                                          if max_duration is not None
                                          else None)
    check_count: Optional[int] = (0
                                  if max_check_count is not None
                                  else None)

    while True:
        start_perf_counter: float = time.perf_counter()
        if max_check_count is not None:
            assert check_count is not None
            check_count += 1
            assert check_count <= max_check_count

        if bool(condition_check(*args, **kwargs)):
            return
        else:
            if (max_duration is not None and time.perf_counter() - c_assert_not_none(start_time) > max_duration
                    or max_check_count is not None and c_assert_not_none(check_count) == max_check_count):
                raise TimeoutError
            else:
                time.sleep(max(0.0, poll_interval - (time.perf_counter() - start_perf_counter)))


def c_identity(el: TYPE) -> TYPE:
    return el


def c_get_and_apply(el:  Union[Callable[..., TYPE], Tuple[Any, str]],
                    params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None,
                    apply_func: Optional[Union[Callable[..., Any], Tuple[Any, str]]] = None,
                    apply_params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None,
                    apply_check_result: Optional[Tuple[Any]] = None,
                    iapply_func: Optional[Union[Callable[..., TYPE], Tuple[Any, str]]] = None,
                    iapply_params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None) -> TYPE:
    if apply_params is not None and apply_func is None:
        raise ValueError
    if iapply_params is not None and iapply_func is None:
        raise ValueError

    el = cast(Callable[..., TYPE], c_normalize_func(func_or_obj_func=el))
    params = c_normalize_params(params=params)

    result: TYPE = el(*params[0], **params[1])

    if apply_func is not None:
        apply_func = c_normalize_func(func_or_obj_func=apply_func)
        apply_params = c_normalize_params(params=apply_params)
        res: Any = apply_func(result, *apply_params[0], **apply_params[1])
        if apply_check_result is not None and res != apply_check_result[0]:
            raise ValueError()

    if iapply_func is not None:
        iapply_func = cast(Callable[..., TYPE], c_normalize_func(func_or_obj_func=iapply_func))
        iapply_params = c_normalize_params(params=iapply_params)
        result = iapply_func(result, *iapply_params[0], **iapply_params[1])

    return result


def c_eq(el_1: Union[Callable[..., Any], Tuple[Any, str]],
         el_2: Union[Callable[..., Any], Tuple[Any, str]],
         params_1: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None,
         params_2: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None) -> bool:

    el_1 = c_normalize_func(func_or_obj_func=el_1)
    args_1, kwargs_1 = c_normalize_params(params=params_1)

    el_2 = c_normalize_func(func_or_obj_func=el_2)
    args_2, kwargs_2 = c_normalize_params(params=params_2)

    return (el_1(*args_1, **kwargs_1) == el_2(*args_2, **kwargs_2)) is True


def c_elem_wise_eq(el_1: Union[Callable[..., con.Pr.Iterable[Any]], Tuple[Any, str]],
                   el_2: Union[Callable[..., con.Pr.Iterable[Any]], Tuple[Any, str]],
                   params_1: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None,
                   params_2: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None) -> bool:

    el_1 = cast(Callable[..., con.Pr.Iterable[Any]], c_normalize_func(func_or_obj_func=el_1))
    args_1, kwargs_1 = c_normalize_params(params=params_1)

    el_2 = cast(Callable[..., con.Pr.Iterable[Any]], c_normalize_func(func_or_obj_func=el_2))
    args_2, kwargs_2 = c_normalize_params(params=params_2)

    ra_1: Any = el_1(*args_1, **kwargs_1)
    if not isinstance(ra_1, con.Pr.Iterable):
        raise ValueError
    ra_1 = con.Ca.c_to_ra(obj=ra_1)

    ra_2: Any = el_2(*args_2, **kwargs_2)
    if not isinstance(ra_2, con.Pr.Iterable):
        raise ValueError
    ra_2 = con.Ca.c_to_ra(obj=ra_2)

    return ((l := len(ra_1)) == len(ra_2)
            and all((ra_1[i] == ra_2[i]) is True
                    for i in range(l)))


def c_raises(raiser: Union[Callable[..., Any], Tuple[Any, str]],
             params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None,
             expected_exception: Type[BaseException] = BaseException) -> bool:

    raiser = c_normalize_func(func_or_obj_func=raiser)
    args, kwargs = c_normalize_params(params=params)

    # noinspection PyBroadException
    try:
        raiser(*args, **kwargs)
    except expected_exception:
        return True
    else:
        return False


def c_and(condition_check_1: Union[Callable[..., bool], Tuple[Any, str]],
          condition_check_2: Union[Callable[..., bool], Tuple[Any, str]],
          params_1: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None,
          params_2: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None) -> bool:

    condition_check_1 = __c_normalize_condition_check(condition_check=condition_check_1)
    args_1, kwargs_1 = c_normalize_params(params=params_1)

    condition_check_2 = __c_normalize_condition_check(condition_check=condition_check_2)
    args_2, kwargs_2 = c_normalize_params(params=params_2)

    return condition_check_1(*args_1, **kwargs_1) and condition_check_2(*args_2, **kwargs_2)


def c_or(condition_check_1: Union[Callable[..., bool], Tuple[Any, str]],
         condition_check_2: Union[Callable[..., bool], Tuple[Any, str]],
         params_1: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None,
         params_2: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None) -> bool:

    condition_check_1 = __c_normalize_condition_check(condition_check=condition_check_1)
    args_1, kwargs_1 = c_normalize_params(params=params_1)

    condition_check_2 = __c_normalize_condition_check(condition_check=condition_check_2)
    args_2, kwargs_2 = c_normalize_params(params=params_2)

    return condition_check_1(*args_1, **kwargs_1) or condition_check_2(*args_2, **kwargs_2)


def c_xor(condition_check_1: Union[Callable[..., bool], Tuple[Any, str]],
          condition_check_2: Union[Callable[..., bool], Tuple[Any, str]],
          params_1: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None,
          params_2: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None) -> bool:

    condition_check_1 = __c_normalize_condition_check(condition_check=condition_check_1)
    args_1, kwargs_1 = c_normalize_params(params=params_1)

    condition_check_2 = __c_normalize_condition_check(condition_check=condition_check_2)
    args_2, kwargs_2 = c_normalize_params(params=params_2)

    return condition_check_1(*args_1, **kwargs_1) ^ condition_check_2(*args_2, **kwargs_2)


def c_not(condition_check: Union[Callable[..., bool], Tuple[Any, str]],
          params: Optional[Tuple[Optional[con.Pr.Iterable[Any]], Optional[con.Pr.Mapping[str, Any]]]] = None) -> bool:

    condition_check = __c_normalize_condition_check(condition_check=condition_check)
    args, kwargs = c_normalize_params(params=params)

    return not condition_check(*args, **kwargs)
