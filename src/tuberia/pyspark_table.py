from typing import Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

from tuberia.table import TableTask
from tuberia.dataframe_table import DataFrameTableTask
from tuberia.base_table import BaseTable


class PySparkTable(BaseTable):
    database: str
    name: str
    path: Optional[str]

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.name}"


PySparkTableTask = TableTask[PySparkTable]
pyspark_table = ...  # TODO: create decorator here.
PySparkDataFrameTableTask = DataFrameTableTask[PySparkTable, StructType, DataFrame]
pyspark_df_table = ...  # TODO: create decorator here.
