from tuberia.table import BaseTable
from tuberia.table_task import TableTask


class PolarsTable(BaseTable):
    path: str


PolarsTableTask = TableTask[PolarsTable]
