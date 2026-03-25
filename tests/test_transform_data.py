import pytest
from pyspark.sql import SparkSession
from src.procesing.transform_data import DataTransformer


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master(
        "local[*]").appName("test").getOrCreate()


def test_transform(spark):
    input_path = "tests/sample_orders.json"
    output_path = "tests/output_parquet"

    transformer = DataTransformer(spark, input_path, output_path)
    df = transformer.extract()
    df_transformed = transformer.transform(df)

    # Validaciones básicas
    assert "total_price" in df_transformed.columns
    assert "year" in df_transformed.columns
    assert "month" in df_transformed.columns
    assert df_transformed.count() > 0
