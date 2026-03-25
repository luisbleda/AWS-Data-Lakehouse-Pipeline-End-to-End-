from config.config_loader import load_config
from pyspark.sql import SparkSession
from utils.constants import (
    AWS_BUCKET, RAW_ORDERS_PATH, PROCESSED_ORDERS_PATH, SPARK_APP_NAME,
    ORDERS_API_URL
)
from procesing.transform_data import DataTransformer
from ingestion.ingest_pipeline import IngestPipeline
from utils.utils import get_value
from jobs.glue_job import GlueETLJob


def main():
    spark = SparkSession.builder.appName("AWS End to End").getOrCreate()
    # Cargar configuración
    config = load_config()

    bucket = get_value(config, AWS_BUCKET)
    raw_prefix = get_value(config, RAW_ORDERS_PATH)
    processed_prefix = get_value(config, PROCESSED_ORDERS_PATH)
    app_name = get_value(config, SPARK_APP_NAME)
    print(f"App name: {app_name}")
    api_url = get_value(config, ORDERS_API_URL)

    input_path = f"s3://{bucket}/{raw_prefix}"
    output_path = f"s3://{bucket}/{processed_prefix}"

    # 1️⃣ Ingestión de datos
    ingest_pipeline = IngestPipeline(api_url, bucket, raw_prefix)
    ingest_pipeline.run()

    # 2️⃣ Pipeline PySpark
    transformer = DataTransformer(
        spark_session=spark,
        input_path=input_path,
        output_path=output_path
    )
    transformer.run()

    # 3️⃣ Glue Job (opcional, si quieres ejecutar ETL Glue)
    glue_job = GlueETLJob(
        spark_context=spark.sparkContext,
        input_path=input_path,
        output_path=output_path
    )
    glue_job.run()

    spark.stop()


if __name__ == "__main__":
    main()
