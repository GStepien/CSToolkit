# rcsd

from __future__ import annotations

import abc
import queue

"""
Raw CS data factory methods for creating low level CS data structures. They are meant for internal usage inside
the concurrency package where using the data structures from the csdata.py module would introduce cyclic
imports.
"""
from typing import Union, Final, Optional, runtime_checkable, Protocol, cast
import multiprocessing.managers as mp_mngr


from concurrency import cs
from utils.types.typevars import TYPE, V_TYPE, K_TYPE
import utils.types.containers as con
import utils.types.magicmethods as mm


_TYPE = Union[mm.Pr.Hashable]


# DEFAULT_CTYPE_PRECISION: Final[con.Pr.Mapping[_TYPE, int]] = FrozenMapping({
#     int: 64,
#     float: 64
# })
#
#
# CTYPE = Type[Union[ctypes.c_float, ctypes.c_double, ctypes.c_longdouble,
#                    ctypes.c_byte, ctypes.c_short, ctypes.c_int, ctypes.c_long, ctypes.c_longlong,
#                    ctypes.c_bool]]


class Ca:
    """
    'c_create_cs_*' methods must return data structures handing cs-synchronization of their internal
    states according to their cs level (e.g., sharing their data across threads or processes).
    The method access to these structures, however, does not have to be atomic (locking is taken care of
    at the higher level data structures from csdata.py).
    """
    # TODO: Add optional support for 'force_ctype' to c_to_cs_value and c_to_mra
    #  (and their counterparts in concurrency.csdata). Note that this overrules the compile time
    #  type annotations.
    # @staticmethod
    # def c_ctype_from_type(cls: Type[Any],
    #                       ctype_precision: Optional[int] = None) -> CTYPE:
    #     if cls == int:
    #         # noinspection PyUnreachableCode
    #         if __debug__:
    #             # noinspection PyUnusedLocal
    #             using_default_ctype_precision: Final[bool] = ctype_precision is None
    #
    #         if ctype_precision is None:
    #             ctype_precision = DEFAULT_CTYPE_PRECISION[int]
    #
    #         if ctype_precision == 8:
    #             return ctypes.c_byte
    #         if ctype_precision == 16:
    #             return ctypes.c_short
    #         if ctype_precision == 32:
    #             return ctypes.c_int
    #         elif ctype_precision == 64:
    #             return ctypes.c_long
    #         elif ctype_precision == 128:
    #             return ctypes.c_longlong
    #         else:
    #             # noinspection PyUnreachableCode
    #             if __debug__:
    #                 assert not using_default_ctype_precision
    #
    #             raise ValueError(f"Unsupported ctype_precision: '{ctype_precision}' for type 'int'.")
    #     elif cls == float:
    #         if ctype_precision is None:
    #             ctype_precision = DEFAULT_CTYPE_PRECISION[float]
    #
    #         if ctype_precision == 32:
    #             return ctypes.c_float
    #         elif ctype_precision == 64:
    #             return ctypes.c_double
    #         elif ctype_precision == 128:
    #             return ctypes.c_longdouble
    #         else:
    #             # noinspection PyUnreachableCode
    #             if __debug__:
    #                 assert not using_default_ctype_precision
    #
    #             raise ValueError(f"Unsupported ctype_precision: '{ctype_precision}' for type 'float'.")
    #     elif cls == bool:
    #         return ctypes.c_bool
    #     else:
    #         raise NotImplementedError(f"Type '{cls.__name__}' currently has no "
    #                                   f"assigned ctype.")

    @staticmethod
    def c_to_cs_value(csl: cs.En.CSL,
                      init_value: TYPE,
                      manager: Optional[mp_mngr.SyncManager]) -> Pr.Value[TYPE]:
        result: Pr.Value[TYPE]

        if csl <= cs.En.CSL.MULTI_THREAD:
            result = Im.ValueSingleThread(init_value=init_value)
        elif csl == cs.En.CSL.MULTI_PROCESS:
            if manager is None:
                raise ValueError(f"Manager required for CSL '{cs.En.CSL.MULTI_PROCESS.name}'")

            # if force_ctype is not None:
            #     tp: Any = getattr(force_ctype, '_type_')
            #     result = manager.Value(tp, init_value)
            # else:

            result = Im.ValueMultiProcess(init_value=init_value,
                                          manager=manager)
        else:
            raise ValueError(f"Unsupported CS level: {csl.name}")

        return result

    @staticmethod
    def c_to_cs_mra(csl: cs.En.CSL,
                    init_values: con.Pr.Iterable[TYPE],
                    manager: Optional[mp_mngr.SyncManager]) -> con.Pr.MutableRandomAccess[TYPE]:
        """
        mra = MutableRandomAccess
        """
        result: con.Pr.MutableRandomAccess[TYPE]

        init_values = list(init_values)

        if csl <= cs.En.CSL.MULTI_THREAD or len(init_values) == 0:
            result = init_values
        elif csl == cs.En.CSL.MULTI_PROCESS:
            if manager is None:
                raise ValueError(f"Manager required for CSL '{cs.En.CSL.MULTI_PROCESS.name}'")

            # if force_ctype is not None:
            #     tp: Any = getattr(force_ctype, '_type_')
            #     """
            #     Typeshed falsely defines manager.Array type as Sequence[Any] while
            #     the actual array proxy is defined as
            #     'ArrayProxy = MakeProxyType('ArrayProxy', ('__len__', '__getitem__', '__setitem__'))'
            #     and is therefore compatible with MutableRandomAccess. Thus, cast is necessary.
            #     """
            #     result = cast(con.Pr.MutableRandomAccess[TYPE], manager.Array(tp, init_values))
            # else:

            result = manager.list(init_values)
        else:
            raise ValueError(f"Unsupported CS level: {csl.name}")

        return result

    @staticmethod
    def c_to_cs_mlseq(csl: cs.En.CSL,
                      init_values: con.Pr.Iterable[TYPE],
                      manager: Optional[mp_mngr.SyncManager]) -> con.Pr.MutableLengthSequence[TYPE]:
        """
        mlseq = MutableLengthSequence
        """
        result: con.Pr.MutableLengthSequence[TYPE]

        init_values = list(init_values)

        if csl <= cs.En.CSL.MULTI_THREAD:
            result = init_values
        elif csl == cs.En.CSL.MULTI_PROCESS:
            if manager is None:
                raise ValueError(f"Manager required for CSL '{cs.En.CSL.MULTI_PROCESS.name}'")

            result = manager.list(init_values)
        else:
            raise ValueError(f"Unsupported CS level: {csl.name}")

        return result

    @staticmethod
    def c_to_cs_mm(csl: cs.En.CSL,
                   init_values: con.Pr.Mapping[K_TYPE, V_TYPE],
                   manager: Optional[mp_mngr.SyncManager]) -> con.Pr.MutableMapping[K_TYPE, V_TYPE]:
        """
        mm = MutableMapping
        """
        result: con.Pr.MutableMapping[K_TYPE, V_TYPE]

        init_values = dict(init_values)

        if csl <= cs.En.CSL.MULTI_THREAD:
            result = init_values
        elif csl == cs.En.CSL.MULTI_PROCESS:
            if manager is None:
                raise ValueError(f"Manager required for CSL '{cs.En.CSL.MULTI_PROCESS.name}'")

            result = manager.dict(init_values)
        else:
            raise ValueError(f"Unsupported CS level: {csl.name}")

        return result

    @staticmethod
    def c_to_cs_queue(csl: cs.En.CSL,
                      init_values: con.Pr.SizedIterable[TYPE],
                      capacity: Optional[int],
                      manager: Optional[mp_mngr.SyncManager]) -> con.Pr.Queue[TYPE]:
        """
        mm = MutableMapping
        """
        if capacity is not None and capacity < 0:
            raise ValueError(f"Negative capacity provided: {capacity}")

        result: con.Pr.Queue[TYPE]

        if capacity is not None and capacity < len(init_values):
            raise ValueError(f"Number of initial values may not exceed queue "
                             f"capacity ({len(init_values)} vs {capacity}).")

        if csl == cs.En.CSL.SINGLE_THREAD or capacity == 0:
            result = con.Im.DequeQueue(capacity=capacity)
        elif csl == cs.En.CSL.MULTI_THREAD:
            result = queue.Queue(maxsize=(capacity
                                          if capacity is not None
                                          else 0))
        elif csl == cs.En.CSL.MULTI_PROCESS:
            if manager is None:
                raise ValueError(f"Manager required for CSL '{cs.En.CSL.MULTI_PROCESS.name}'")

            result = manager.Queue(maxsize=(capacity
                                            if capacity is not None
                                            else 0))
            # Only accepts int, zero is treated as infinite
            # Zero capacity already treated as <= MULTI_THREAD case so here
            # we either have capacity of None or > 0.
        else:
            raise ValueError(f"Unsupported CS level: {csl.name}")

        # noinspection PyTypeChecker
        for val in init_values:
            result.put(val, False)
        return result


class Pr:

    @runtime_checkable
    class Value(Protocol[TYPE]):
        """
        Protocol mimicking manager.ValueProxy.
        """
        @property
        @abc.abstractmethod
        def value(self, /) -> TYPE:
            ...

        @value.setter
        def value(self,
                  new_value: TYPE, /) -> None:
            # See: https://github.com/python/mypy/issues/4165
            # We cannot decorate this method with abc.abstractmethod, but still want to enforce implementation.
            raise NotImplementedError


class Im:
    class ValueSingleThread(Pr.Value[TYPE]):
        __f_value: TYPE

        def __init__(self,
                     init_value: TYPE):
            self.__f_value = init_value

        @property
        def value(self, /) -> TYPE:
            return self.__f_value

        @value.setter
        def value(self,
                  new_value: TYPE, /) -> None:
            self.__f_value = new_value

    class ValueMultiProcess(Pr.Value[TYPE]):
        """
        Note that this class internally uses a managed namespace to store the value.
        When setting a new value or accessing the current one, the whole object is pickled and
        transferred to and from the manager server. Use this class as a fallback if no other,
        more performant solution exists/can be implemented at the moment.
        """

        __f_namespace: Final[mp_mngr.Namespace]

        def __init__(self,
                     init_value: TYPE,
                     manager: mp_mngr.SyncManager):
            self.__f_namespace = manager.Namespace()
            setattr(self.__f_namespace, "value", init_value)

        @property
        def value(self, /) -> TYPE:
            return cast(TYPE, getattr(self.__f_namespace, "value"))

        @value.setter
        def value(self,
                  new_value: TYPE, /) -> None:
            setattr(self.__f_namespace, "value", new_value)
