from typing import Any, Callable, Optional, Union, overload

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

from tuberia.dataframe_table import DataFrameTableFunctionTask
from tuberia.table import BaseTable
from tuberia.table_task import TableTask


class PySparkTable(BaseTable):
    database: str
    name: str
    path: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.name}"


PySparkTableTask = TableTask[PySparkTable]
pyspark_table = ...  # TODO: create decorator here.
PySparkDataFrameTableFunctionTask = DataFrameTableFunctionTask[
    PySparkTable, StructType, DataFrame
]


@overload
def pyspark_df_table(
    define: Callable[..., DataFrame]
) -> PySparkDataFrameTableFunctionTask:
    ...


@overload
def pyspark_df_table(
    define: Optional[Callable[..., DataFrame]] = None,
    schema: Optional[StructType] = None,
    persist: Optional[Callable[[DataFrame], PySparkTable]] = None,
    validate: Optional[Callable[[PySparkTable], None]] = None,
    **task_init_kwargs: Any,
) -> Callable[[Callable[..., DataFrame]], PySparkDataFrameTableFunctionTask]:
    ...


def pyspark_df_table(
    define: Optional[Callable[..., DataFrame]] = None,
    schema: Optional[StructType] = None,
    persist: Optional[Callable[[DataFrame], PySparkTable]] = None,
    validate: Optional[Callable[[PySparkTable], None]] = None,
    **task_init_kwargs: Any,
) -> Union[
    PySparkDataFrameTableFunctionTask,
    Callable[[Callable[..., DataFrame]], PySparkDataFrameTableFunctionTask],
]:
    if define is None:
        return lambda define: PySparkDataFrameTableFunctionTask(
            define=define,
            schema=schema,
            persist=persist,
            validate=validate,
            **task_init_kwargs,
        )
    return PySparkDataFrameTableFunctionTask(define=define, **task_init_kwargs)
