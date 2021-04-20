from __future__ import annotations

from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Hashable,
    List,
    Optional,
    TypeVar,
)
import threading

import dask
import dask.utils

KT = TypeVar("KT", bound=Hashable)
VT = TypeVar("VT")


class NoPickleDict(Dict[KT, VT]):
    def __getstate__(self):
        return ()

    def __setstate__(self, state):
        pass


T = TypeVar("T")


class OncePerWorker(Generic[T]):
    """
    Wraps the result of a function, ensuring it's called at most once.

    Attribute access on `OncePerWorker` forwards to the object returned by the function.
    At the time of the first attribute access, the function is called. Subsequent attribute
    access will reuse the same object.

    Within a process, unpicking a `OncePerWorker` instance always resolves to the same object.

    Do not use directly; you should use the `once_per_worker` function instead.
    If you do use directly, construct with `instance_for_function`, not ``__init__``.
    """

    _instances: ClassVar[NoPickleDict[str, OncePerWorker]] = NoPickleDict()
    _instances_lock = dask.utils.SerializableLock("OncePerWorker-lock")

    _func: Callable[[], T]
    _token: str
    _value: Optional[T]
    _lock: threading.Lock

    @classmethod
    def instance_for_function(
        cls, func: Callable[[], T], token: Optional[str] = None
    ) -> OncePerWorker:
        """
        Get the `OncePerWorker` instance for a function, creating one if necessary.

        Within the same process, calling `instance_for_function` multiple times
        with the same ``func`` or ``token`` will always return the same value.
        """
        token_ = token or dask.base.tokenize(func)

        with cls._instances_lock:
            try:
                return cls._instances[token_]
            except KeyError:
                pass

            cls._instances[token_] = instance = cls(func, token_)
            return instance

    def __reduce__(self):
        return (self.instance_for_function, (self._func, self._token))

    def __init__(self, func: Callable[[], T], token: str) -> None:
        self._func = func
        self._token = token
        self._lock = threading.Lock()
        self._value = None

    def _get_value(self) -> T:
        if self._value is not None:
            # don't need lock here, because once value is set once, it won't be mutated again
            return self._value

        with self._lock:
            if self._value is None:
                # We won the lock race; create `self._value`
                self._value = self._func()
            assert self._value is not None
            return self._value

    def __getattr__(self, attr: str) -> T:
        if attr in ("__dask_graph__", "__name__", "__dask_tokenize__"):
            raise AttributeError(attr)
        return getattr(self._get_value(), attr)

    def __dir__(self) -> List[str]:
        return dir(self._value)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} at {hex(id(self))} of {self._value!r}>"


def once_per_worker(func: Callable[[], Any]):
    """
    Create a Delayed object for the return value of ``func``, which runs at most once per process.

    ``func`` must take no arguments.

    Example
    -------
    >>> import time
    >>> import os
    >>> import distributed
    >>> from once_per_worker import once_per_worker
    >>>
    >>> def v_slow(x: int) -> int:
    ...     print(f"sleeping for {x} seconds on PID {os.getpid()}")
    ...     time.sleep(x)
    ...     return x
    >>> slow_result = once_per_worker(lambda: v_slow(5))
    >>> many_slows = sum([slow_result] * 10)
    >>>
    >>> client = distributed.Client(processes=True)
    >>> # This should only take ~5sec, and you shouldn't see the same PID sleep more than once.
    >>> many_slows.compute()
    """
    return dask.delayed(
        OncePerWorker.instance_for_function(func), pure=True, traverse=False
    )
