import pytest
from pyspark.sql import SparkSession
from jobs.glue_job import GlueETLJob


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master(
        "local[*]").appName("glue_test").getOrCreate()


def test_glue_etl_run(spark, tmp_path):
    input_path = "tests/data_sample/orders.json"
    output_path = tmp_path / "glue_output"

    job = GlueETLJob(
        spark_context=spark.sparkContext,
        input_path=input_path,
        output_path=str(output_path)
    )
    job.run()

    # Comprobar que se generaron ficheros Parquet
    assert len(list(output_path.glob("*.parquet"))) > 0
