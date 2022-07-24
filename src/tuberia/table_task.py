from typing import Any, Callable, Generic, TypeVar, Union, overload

import prefect
from prefect.tasks.core.function import FunctionTask

from tuberia.table import BaseTable

_TTable = TypeVar("_TTable", bound=BaseTable)


class TableTask(prefect.Task, Generic[_TTable]):
    def run(self, **_: Any) -> _TTable:
        raise NotImplementedError()


class TableFunctionTask(TableTask[_TTable], FunctionTask):
    def __init__(self, fun: Callable[..., _TTable], **kwargs):
        super().__init__(fun, **kwargs)


# Defining decorator. Inspired by prefect/utilities/tasks.py:
# To support type checking with optional arguments to `table`, we need to make
# use of `typing.overload`.
@overload
def table(fun: Callable[..., _TTable]) -> TableFunctionTask[_TTable]:
    ...


@overload
def table(
    fun: Callable[..., _TTable] = None,
    **task_init_kwargs: Any,
) -> Callable[[Callable[..., _TTable]], TableFunctionTask[_TTable]]:
    ...


def table(
    fun: Callable[..., _TTable] = None, **task_init_kwargs: Any
) -> Union[
    TableFunctionTask[_TTable],
    Callable[[Callable[..., _TTable]], TableFunctionTask[_TTable]],
]:
    if fun is None:
        return lambda fun: TableFunctionTask[_TTable](
            fun=fun, **task_init_kwargs
        )
    return TableFunctionTask[_TTable](fun=fun, **task_init_kwargs)
