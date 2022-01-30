# decs

from typing import Generic, final, Any, cast, Type

from utils.types.typevars import TYPE


# Code below is currently unmaintained.
# class _Im:
#     class AnySubscriptableDecorator(Generic[TYPE]):
#         """
#         For a non-type object x and `y = AnySubscriptableDecorator(x)`:
#
#         type(b) == b.__class__ == AnySubscriptableDecorator
#
#         In order to get a real subclass of both AnySubscriptableDecorator and a.__class__
#         (i.e., with type(b) being a subclass of both a.__class__ and AnySubscriptableDecorator),
#         use Ca.c_make_any_subscriptable.
#         """
#
#         # Long name on purpose in order to minimize name collision probability
#         __f_subscriptably_wrapped_original_obj: TYPE
#
#         def __init__(self,
#                      obj: TYPE):
#             object.__setattr__(self, '__f_subscriptably_wrapped_original_obj', obj)
#
#         @final
#         def __getitem__(self, /,
#                         *_: Any,
#                         **__: Any) -> TYPE:
#             return cast(TYPE, object.__getattribute__(self, '__f_subscriptably_wrapped_original_obj'))
#
#         @final
#         def __getattribute__(self,
#                              name: str, /) -> Any:
#             if (name == '__getitem__'
#                 or (name == '__class__'
#                     and not isinstance(object.__getattribute__(self, '__f_subscriptably_wrapped_original_obj'), type))):
#                 """
#                 __getitem__ intercepted by this class' __getitem__ which simply returns the wrapped object.
#                 __class__ intercepted by this class' __class__ if wrapped object is not a type (in case of type object,
#                 the wrapper should be "transparent").
#                 Everything else is forwarded to wrapped object.
#                 """
#                 return object.__getattribute__(self, name)
#             else:
#                 return getattr(object.__getattribute__(self, '__f_subscriptably_wrapped_original_obj'),
#                                name)
#
#         @final
#         def __setattr__(self,
#                         name: str,
#                         value: Any, /) -> None:
#             if name == '__getitem__':
#                 raise ValueError(f"Cannot re-set attribute '__getitem__' for instance of"
#                                  f"class '{self.__class__.__name__}'.")
#             else:
#                 setattr(object.__getattribute__(self, '__f_subscriptably_wrapped_original_obj'), name, value)
#
#         @final
#         def __delattr__(self,
#                         name: str, /) -> None:
#             if name == '__getitem__':
#                 raise ValueError(f"Cannot delete attribute '__getitem__' for instance of"
#                                  f"class '{self.__class__.__name__}'.")
#             else:
#                 delattr(object.__getattribute__(self, '__f_subscriptably_wrapped_original_obj'), name)
#
#
# def c_make_any_subscriptable(obj: TYPE) -> TYPE:
#     if isinstance(obj, type):
#         return cast(TYPE, _Im.AnySubscriptableDecorator(obj=obj))
#     else:
#         new_type: Type[TYPE] = cast(Type[TYPE],
#                                     type(f"{obj.__class__.__name__}"
#                                          f"WrappedIn{_Im.AnySubscriptableDecorator.__name__}",
#                                          (_Im.AnySubscriptableDecorator, obj.__class__),
#                                          {}))
#         return cast(TYPE, new_type(obj=obj))  # type: ignore
