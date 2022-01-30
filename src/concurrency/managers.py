"""
Note: Regular Managers cannot be pickled. This module adds support for picklable
managers so that subprocesses can create data structures managed by a common parent manager.
"""

from __future__ import annotations

import abc
from typing import Any, Optional, final, Callable, Tuple, Final, Protocol, runtime_checkable
import multiprocessing.managers as mp_mngr

import utils.types.containers as con
from concurrency import cs


class Im:
    class PicklableSyncManagedMixin(cs.Im.HasCSLMixin):
        __f_picklable_sync_manager: Final[Optional[Im.PicklableSyncManager]]

        def __init__(self,
                     *args: Any,
                     csl: cs.En.CSL,
                     picklable_sync_manager: Optional[Im.PicklableSyncManager],
                     **kwargs: Any):
            if (csl >= cs.En.CSL.MULTI_PROCESS) != (picklable_sync_manager is not None):
                raise ValueError(f"A {Im.PicklableSyncManager.__name__} must be provided "
                                 f"if and only if csl >= {cs.En.CSL.MULTI_PROCESS.name}.")
            self.__f_picklable_sync_manager = picklable_sync_manager
            super().__init__(*args,
                             csl=csl,
                             **kwargs)

        @final
        def _c_get_manager(self) -> Optional[Im.PicklableSyncManager]:
            return self.__f_picklable_sync_manager

    class PicklableSyncManager(mp_mngr.SyncManager):
        __f_address: Optional[Any]
        __f_is_proxy: Optional[bool]
        __f_was_pickled: bool

        def __init__(self,
                     address: Optional[Any] = None,
                     authkey: Optional[bytes] = None):
            if authkey is not None:
                raise NotImplementedError(f"'authkey' support not (yet) implemented for "
                                          f"{Im.PicklableSyncManager.__name__}.")
            super().__init__(address=address,
                             authkey=authkey)

            self.__c_init(address=address)

        @final
        def __c_init(self,
                     address: Optional[Any]) -> None:
            self.__f_address = address
            self.__f_is_proxy = None
            self.__f_was_pickled = False

        @final
        @property
        def is_proxy(self) -> Optional[bool]:
            """
            None = Not decided yet (neither start() nor connect() called yet), False = Is server, True = Is Proxy
            """

            return self.__f_is_proxy

        @final
        @property
        def init_address(self) -> Optional[Any]:
            return self.__f_address

        def connect(self) -> None:
            if self.__f_address is None:
                raise ValueError(f"Cannot connect to 'None' address.")
            super().connect()
            self.__f_is_proxy = True

        def start(self,
                  initializer: Optional[Callable[..., Any]] = None,
                  initargs: con.Pr.Iterable[Any] = ()) -> None:
            if self.__f_was_pickled:
                raise ValueError(f"Cannot start a manager that has been pickled and unpickled before. "
                                 f"Start original manger instance and use this instance to connect to it.")

            super().start(initializer=initializer,
                          initargs=initargs)
            self.__f_is_proxy = False

        def __getstate__(self) -> Tuple[Any, ...]:
            address: Any
            if self.__f_is_proxy is True:
                assert self.__f_address is not None and self.__f_address == self.address
                # If this is a proxy, it must be connected to some BaseManager Server (i.e., an address must have
                # been provided in the first place),
                # get address of this server
                address = self.__f_address
            elif self.__f_is_proxy is False:
                address = self.address
                assert address is not None and (self.__f_address is None or self.__f_address == address)
                # If this is a server, it must have an address on which it is listening (either a provided one
                # or an address chosen by the server if no address was provided in the first place)
                # get address of this server
            else:
                assert self.__f_is_proxy is None and self.__f_address == self.address
                # The above must be True if provided address is None (server not started yet -> has no address yet)
                # as well as if an address was provided.

                # This instance has neither been started nor connected. Pickling only makes
                # sense if we have an address to connect to after unpickling (otherwise we have two independent
                # manager server instances):
                if self.__f_address is None:
                    raise ValueError(f"Cannot pickle manager instance if no server address is known (yet). "
                                     f"Start this instance and retry.")
                else:
                    address = self.__f_address
            assert address is not None

            return (address,
                    self.__f_is_proxy)

        def __setstate__(self, state: Tuple[Any, ...]) -> None:
            assert (isinstance(state, tuple)
                    and len(state) == 2
                    and state[0] is not None
                    and (state[1] is None or isinstance(state[1], bool)))

            address: Final[Any] = state[0]
            is_proxy: Final[Optional[bool]] = state[1]

            super().__init__(address=address,
                             authkey=None)

            self.__c_init(address=address)

            assert self.__f_address == address and self.__f_is_proxy is None and not self.__f_was_pickled
            self.__f_was_pickled = True

            if isinstance(is_proxy, bool):
                # Connect to the address of the other manager instance (independent whether
                # the other one is itself a proxy or a server instance).
                self.__f_is_proxy = True
                self.connect()
            else:
                assert is_proxy is None
                # Nothing else to do here
