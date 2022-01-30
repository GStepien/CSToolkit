from typing import Any, Dict, Tuple, Callable, Final

import pytest

from utils.dynamic.inspection import c_get_callable_param_getter, ParamValue, UnknownParamError, MissingNonDefaultError, \
    c_get_self_getter

_DEFAULT_ARG6: Final[float] = 5.6


# noinspection PyUnusedLocal
def __foo(arg0: int, arg1: int, arg2: str, /,
          arg3: float, arg4: str, *,
          arg5: int, arg6: float = _DEFAULT_ARG6) -> None:
    return


def test_param_getter() -> None:
    param_getter: Final[Callable[[str, Tuple[Any, ...], Dict[str, Any]], ParamValue]] = c_get_callable_param_getter(
        callable_=__foo
    )

    assert param_getter('arg6',
                        (1, 3, "asdf", 4.5),
                        {"arg4": "aaa", "arg5": -5}) == ParamValue(position=None,
                                                                   value=_DEFAULT_ARG6)

    with pytest.raises(UnknownParamError):
        assert param_getter('arg7',
                            (1, 3, "asdf", 4.5),
                            {"arg4": "aaa", "arg5": -5}) == ParamValue(position=None,
                                                                       value=_DEFAULT_ARG6)

    with pytest.raises(MissingNonDefaultError):
        assert param_getter('arg5',
                            (1, 3, "asdf", 4.5),
                            {"arg4": "aaa"}) == ParamValue(position=None,
                                                           value=_DEFAULT_ARG6)

    assert param_getter('arg2',
                        (1, 3, "asdf", 4.5),
                        {"arg4": "aaa", "arg5": -5}) == ParamValue(position=2,
                                                                   value="asdf")

    assert param_getter('arg6',
                        (1, 3, "asdf", 4.5),
                        {"arg4": "aaa", "arg5": -5, "arg6": 1.555}) == ParamValue(position="arg6",
                                                                                  value=1.555)

    assert param_getter('arg5',
                        (1, 3, "asdf", 4.5),
                        {"arg4": "aaa", "arg5": -5, "arg6": 1.555}) == ParamValue(position="arg5",
                                                                                  value=-5)

    assert param_getter('arg5',
                        (1, 3, "asdf", 4.5),
                        {"arg4": "aaa", "arg5": -5}) == ParamValue(position="arg5",
                                                                   value=-5)

    with pytest.raises(AssertionError):
        assert param_getter('arg5',
                            (1, 3, "asdf", 4.5),
                            {"arg4": "aaa", "arg5": -5}) == ParamValue(position="arg5",
                                                                       value=-55)


class Foo:

    # noinspection PyMethodParameters
    def foo1(self_arg, arg0: int, arg1: str) -> None:
        return

    # noinspection PyMethodParameters
    def foo2(self, arg0: int, arg1: str) -> None:
        return

    # noinspection PyMethodParameters
    def foo3(self_arg, /, arg0: int, arg1: str) -> None:
        return

    # noinspection PyMethodParameters
    def foo4(self_arg, arg0: int, *, arg1: str) -> None:
        return


def test_self_getter() -> None:
    self_getter: Callable[[Tuple[Any, ...], Dict[str, Any]], ParamValue]
    foo: Final[Foo] = Foo()

    self_getter = c_get_self_getter(Foo.foo1)
    assert self_getter((foo, 1, "asdf"),
                       {}) == ParamValue(position=0,
                                         value=foo)

    self_getter = c_get_self_getter(Foo.foo2)
    assert self_getter((foo, 1, "asdf"),
                       {}) == ParamValue(position=0,
                                         value=foo)

    self_getter = c_get_self_getter(Foo.foo3)
    assert self_getter((foo,),
                       {"arg0": 1, "arg1": "asdf"}) == ParamValue(position=0,
                                                                  value=foo)

    self_getter = c_get_self_getter(Foo.foo4)
    assert self_getter((foo, 1, "asdf"),
                       {}) == ParamValue(position=0,
                                         value=foo)
