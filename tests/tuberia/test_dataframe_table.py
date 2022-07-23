from prefect import Flow

from tuberia.dataframe_table import df_table
from tuberia.pyspark_table import PySparkTable


def test_dataframe_table_task(spark):
    @df_table(
        persist=lambda df: PySparkTable(
            database="my_database",
            name="_".join(sorted([str(i.id) for i in df.collect()])),
        )
    )
    def range():
        return spark.range(2)

    with Flow("test") as flow:
        output = range()

    status = flow.run()
    assert status
    output_table = status.result[output].result
    assert output_table.full_name == "my_database.0_1"
