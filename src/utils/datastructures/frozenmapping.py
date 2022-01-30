from typing import Tuple, Any, Final

import utils.types.containers as con
from utils.types.typevars import V_TYPE_co, K_TYPE


class FrozenMapping(con.Ab.Mapping[K_TYPE, V_TYPE_co]):

    def __init__(self,
                 mapping: con.Pr.Mapping[K_TYPE, V_TYPE_co],
                 as_view: bool = True):
        self.__f_mapping: Final[con.Pr.Mapping[K_TYPE, V_TYPE_co]] = (mapping
                                                                      if as_view
                                                                      else dict(mapping))

    def _c_eq(self, value: Any, /) -> bool:
        return self.__f_mapping.__eq__(value)

    def __getitem__(self, key: K_TYPE, /) -> V_TYPE_co:
        return self.__f_mapping.__getitem__(key)

    def keys(self, /) -> con.Pr.Set[K_TYPE]:
        return self.__f_mapping.keys()

    def items(self, /) -> con.Pr.Set[Tuple[K_TYPE, V_TYPE_co]]:
        return self.__f_mapping.items()

    def values(self, /) -> con.Pr.ValuesView[V_TYPE_co]:
        return self.__f_mapping.values()
