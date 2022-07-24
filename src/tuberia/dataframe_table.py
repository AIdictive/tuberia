from typing import Any, Callable, Generic, Optional, TypeVar

from makefun import wraps
from prefect.core.task import _validate_run_signature

from tuberia.table_task import TableTask, _TTable

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
        schema: Optional[_TSchema] = None,
        persist: Optional[Callable[[_TDataFrame], _TTable]] = None,
        validate: Optional[Callable[[_TTable], None]] = None,
        **kwargs: Any,
    ):
        self.schema = schema
        if define is not None:
            _validate_run_signature(define)
            self.run = wraps(define)(self.run)
            self.define = define
        if persist is not None:
            self.persist = persist
        if validate is not None:
            self.validate = validate
        super().__init__(**kwargs)
