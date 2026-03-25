from pyspark.sql.functions import col, explode, year, month
from pyspark.sql import DataFrame


class DataTransformer:

    def __init__(self, spark_session, input_path, output_path):
        """
        :param spark_session: SparkSession existente
        :param input_path: ruta de entrada (S3 o local)
        :param output_path: ruta de salida (S3 o local)
        """
        self.spark = spark_session
        self.input_path = input_path
        self.output_path = output_path

    def extract(self) -> DataFrame:
        """Lee los datos JSON desde la ruta de entrada"""
        print(f"[Transform] Extracting data from {self.input_path}")
        df = self.spark.read.json(self.input_path)
        print(f"[Transform] Extracted {df.count()} rows")
        return df

    def validate(self, df: DataFrame) -> DataFrame:
        """Valida columnas obligatorias y elimina duplicados"""
        required_columns = ["id", "userId", "date", "products"]
        missing_cols = [c for c in required_columns if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        df = df.dropDuplicates(["id"])
        df = df.filter(col("products").isNotNull())
        print(f"[Transform] After validation: {df.count()} rows")
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """Realiza la transformación avanzada de los datos"""
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

        # Calcular total_price por producto
        df_final = df_final.withColumn(
            "total_price", col("quantity") * col("unit_price"))

        # Particionar por año y mes para optimización Athena
        df_final = df_final.withColumn("year", year(col("date")))
        df_final = df_final.withColumn("month", month(col("date")))

        print(f"Transformation complete with {df_final.count()} rows")
        return df_final

    def load(self, df: DataFrame):
        """Guarda los datos transformados en parquet particionado"""
        df.write.mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(self.output_path)
        print(f"[Transform] {self.output_path} (partitioned by year/month)")

    def run(self):
        df = self.extract()
        df_transformed = self.transform(df)
        self.load(df_transformed)
