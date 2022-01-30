from typing import Optional, Any, Callable, cast, Type

from utils.types.typevars import TYPE


def c_assert_not_none(obj: Optional[TYPE]) -> TYPE:
    assert obj is not None
    return obj


def c_assert_callable(obj: Any) -> Callable[..., Any]:
    assert callable(obj)
    return cast(Callable[..., Any], obj)


def c_assert_type(obj: Any,
                  cls: Type[TYPE]) -> TYPE:
    assert isinstance(obj, cls)
    return obj
