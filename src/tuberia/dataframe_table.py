from typing import Any, Callable, Optional, Union, overload, Generic, TypeVar

from makefun import wraps
from prefect.core.task import _validate_run_signature

from tuberia.table import _TTable, TableTask


_TSchema = TypeVar("_TSchema")
_TDataFrame = TypeVar("_TDataFrame")


class DataFrameTableTask(
    TableTask[_TTable], Generic[_TTable, _TSchema, _TDataFrame]
):
    def get_table_name(self, **kwargs) -> str:
        return self.name.format(**kwargs)

    def define(self, **_: Any) -> _TDataFrame:
        raise NotImplementedError()

    def persist(self, _: _TDataFrame, /) -> _TTable:
        raise NotImplementedError()

    def validate(self, _: _TTable, /) -> None:
        pass

    def run(self, **kwargs: Any) -> _TTable:
        df = self.define(**kwargs)
        table = self.persist(df)
        self.validate(table)
        return table


class DataFrameTableFunctionTask(
    DataFrameTableTask[_TTable, _TSchema, _TDataFrame]
):
    def __init__(
        self,
        define: Optional[Callable[..., _TDataFrame]] = None,
        persist: Optional[Callable[[_TDataFrame], _TTable]] = None,
        validate: Optional[Callable[[_TTable], None]] = None,
        **kwargs: Any,
    ):
        if define is not None:
            _validate_run_signature(define)
            self.run = wraps(define)(self.run)
            self.define = define
        if persist is not None:
            self.persist = persist
        if validate is not None:
            self.validate = validate
        super().__init__(**kwargs)


# Taken from prefect/utilities/tasks.py:
# To support type checking with optional arguments to `table`, we need to make
# use of `typing.overload`
@overload
def df_table(
    fun: Callable[..., _TDataFrame]
) -> DataFrameTableFunctionTask[_TTable, _TSchema, _TDataFrame]:
    ...


@overload
def df_table(
    define: Optional[Callable[..., _TDataFrame]] = None,
    schema: Optional[_TSchema] = None,
    persist: Optional[Callable[[_TDataFrame], _TTable]] = None,
    validate: Optional[Callable[[_TTable], None]] = None,
    **task_init_kwargs: Any,
) -> Callable[
    [Callable[..., _TDataFrame]],
    DataFrameTableFunctionTask[_TTable, _TSchema, _TDataFrame],
]:
    ...


def df_table(
    fun: Callable[..., _TDataFrame] = None, **task_init_kwargs: Any
) -> Union[
    DataFrameTableFunctionTask,
    Callable[[Callable[..., _TDataFrame]], DataFrameTableFunctionTask],
]:
    if fun is None:
        return lambda fun: DataFrameTableFunctionTask(
            define=fun, **task_init_kwargs
        )
    return DataFrameTableFunctionTask(define=fun, **task_init_kwargs)
