import time
from functools import wraps
from typing import Optional, Callable, Any, Final, cast

from utils.types.typevars import CALLABLE_TYPE


class UnexpectedDuration(AssertionError):
    pass


class TooShortDuration(UnexpectedDuration):
    pass


class TooLongDuration(UnexpectedDuration):
    pass


def de_assert_duration(min_sec: Optional[float],
                       max_sec: Optional[float]) -> Callable[[CALLABLE_TYPE], CALLABLE_TYPE]:

    if min_sec is not None and min_sec < 0:
        raise ValueError(f"'min_sec' is out of bounds: {min_sec}")
    elif max_sec is not None and max_sec < 0:
        raise ValueError(f"'max_sec' is out of bounds: {max_sec}")
    elif min_sec is not None and max_sec is not None and min_sec > max_sec:
        raise ValueError(f"'min_sec' ({min_sec}) may not be larger than 'max_sec' ({max_sec}).")

    def de_assert_duration_(func: CALLABLE_TYPE) -> CALLABLE_TYPE:
        if min_sec is None and max_sec is None:
            return func

        @wraps(func)
        def c_wrapping_func(*args: Any, **kwargs: Any) -> Any:
            start_time: Final[float] = time.time()
            exception: Optional[Exception] = None
            result: Any = None
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                exception = e

            # noinspection PyUnusedLocal
            duration: Final[float] = time.time() - start_time

            # noinspection PyUnreachableCode
            if __debug__:
                if min_sec is not None and duration < min_sec:
                    raise TooShortDuration(f"Execution duration ({duration} s) was shorter "
                                           f"than 'min_sec' ({min_sec} s).") from exception

                if max_sec is not None and duration > max_sec:
                    raise TooLongDuration(f"Execution duration ({duration} s) "
                                          f"exceeded 'max_sec' ({max_sec} s).") from exception

            if exception is not None:
                raise exception
            else:
                return result

        return cast(CALLABLE_TYPE, c_wrapping_func)

    return de_assert_duration_
