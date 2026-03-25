from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, explode, year, month
from pyspark.sql import DataFrame


class GlueETLJob:
    def __init__(self, spark_context: SparkContext,
                 input_path: str, output_path: str):
        """
        Clase para ETL usando AWS GlueContext
        :param spark_context: SparkContext existente
        :param input_path: Ruta de entrada en S3
        :param output_path: Ruta de salida en S3
        """
        self.sc = spark_context
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.input_path = input_path
        self.output_path = output_path

    def extract(self) -> DataFrame:
        """Leer datos desde S3 (JSON)"""
        df = self.spark.read.json(self.input_path)
        print(f"[Glue Job] Extracted {df.count()} rows from {self.input_path}")
        return df

    def validate(self, df: DataFrame) -> DataFrame:
        """Valida columnas obligatorias y elimina duplicados"""
        required_columns = ["id", "userId", "date", "products"]
        missing_cols = [c for c in required_columns if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        df = df.dropDuplicates(["id"])
        df = df.filter(col("products").isNotNull())
        print(f"[Glue Job] After validation: {df.count()} rows")
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """Transformación avanzada: explode productos, total_price"""
        df = self.validate(df)

        # Explode productos
        df_exploded = df.withColumn("product", explode(col("products")))

        # Selección y renombrado de columnas
        df_final = df_exploded.select(
            col("id").alias("order_id"),
            col("userId").alias("customer_id"),
            col("date"),
            col("product.productId").alias("product_id"),
            col("product.quantity").alias("quantity"),
            col("product.price").alias("unit_price")
        )

        # Calcular total_price
        df_final = df_final.withColumn(
            "total_price", col("quantity") * col("unit_price"))

        # Particionar por año y mes
        df_final = df_final.withColumn("year", year(col("date")))
        df_final = df_final.withColumn("month", month(col("date")))

        print(f"[Glue Job] Transformation complete with {df_final.count()}")
        return df_final

    def load(self, df: DataFrame):
        """Guardar datos procesados en S3 (Parquet particionado)"""
        df.write.mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(self.output_path)
        print(f"[Glue Job] Data loaded to {self.output_path}")

    def run(self):
        df = self.extract()
        df_transformed = self.transform(df)
        self.load(df_transformed)
