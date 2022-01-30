import inspect
from dataclasses import dataclass
from typing import Tuple, Any, Dict, Optional, Union, Final, Callable

from utils.types.typevars import CALLABLE_TYPE


@dataclass(frozen=True)
class ParamValue:
    position: Optional[Union[int, str]]
    """
    position: 
    - int: value index in args
    - str: value key in kwargs
    - None: parameter was not provided but default value was present
    """
    value: Any


class CallableSignatureError(RuntimeError):
    pass


class UnknownParamError(ValueError):
    pass


class MissingNonDefaultError(ValueError):
    pass


def c_get_callable_param_getter(callable_: CALLABLE_TYPE) \
        -> Callable[[str, Tuple[Any, ...], Dict[str, Any]], ParamValue]:

    params_map: Dict[str, inspect.Parameter] = dict(inspect.signature(callable_).parameters)
    param_names_tuple: Tuple[str, ...] = tuple(param_name
                                               for param_name in params_map)

    params_default: Final[Dict[str, Any]] = \
        dict((param_name, default)
             for param_name in param_names_tuple
             if (default := params_map[param_name].default) != inspect.Parameter.empty)

    params_pos: Final[Dict[str, int]] = dict(
        (param_names_tuple[pos], pos) for pos in range(len(param_names_tuple))
    )

    del params_map, param_names_tuple

    def _c_call_param_getter(param_name: str,
                             args: Tuple[Any, ...],
                             kwargs: Dict[str, Any]) -> ParamValue:

        if param_name not in params_pos:
            raise UnknownParamError(f"Unknown parameter '{param_name}'.")
        elif param_name in kwargs:
            return ParamValue(position=param_name,
                              value=kwargs[param_name])
        elif (pos := params_pos[param_name]) < len(args):
            return ParamValue(position=pos,
                              value=args[pos])
        elif param_name in params_default:
            return ParamValue(position=None,
                              value=params_default[param_name])
        else:
            raise MissingNonDefaultError(f"No default value for missing '{param_name}' parameter.")

    return _c_call_param_getter


def c_get_self_getter(callable_: CALLABLE_TYPE,
                      _call_param_getter: Optional[Callable[[str, Tuple[Any, ...], Dict[str, Any]],
                                                            ParamValue]] = None) \
        -> Callable[[Tuple[Any, ...], Dict[str, Any]], ParamValue]:
    """
    Assumes callable_ to be unbound and its first parameter to be a reference to the object to which method is
    later bound (i.e., the 'self' parameter, although it can be named differently).

    If provided, '_call_param_getter' should be the result of c_get_callable_param_getter(callable_).
    This parameter can be used in order to avoid duplicate calls to c_get_callable_param_getter
    if the latter's result is already known by the caller.
    """
    params_map: Dict[str, inspect.Parameter] = dict(inspect.signature(callable_).parameters)
    if len(params_map) == 0:
        raise ValueError("Provided method has no parameters.")
    self_param_name: Final[str] = next(iter(params_map))
    del params_map

    if _call_param_getter is None:
        _call_param_getter = c_get_callable_param_getter(callable_=callable_)

    def _c_self_getter(args: Tuple[Any, ...],
                       kwargs: Dict[str, Any]) -> ParamValue:
        assert _call_param_getter is not None
        return _call_param_getter(self_param_name, args, kwargs)

    return _c_self_getter
