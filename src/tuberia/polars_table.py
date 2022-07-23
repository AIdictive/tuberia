from tuberia.table import TableTask
from tuberia.base_table import BaseTable


class PolarsTable(BaseTable):
    path: str


PolarsTableTask = TableTask[PolarsTable]
