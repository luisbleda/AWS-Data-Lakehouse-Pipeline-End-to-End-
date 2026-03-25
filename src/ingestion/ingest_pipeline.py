import json
import requests
import boto3
from datetime import datetime


class IngestPipeline:
    def __init__(self, api_url: str, bucket: str, prefix: str):
        """
        Clase para la ingesta de datos desde API a S3
        :param api_url: URL de la API a consultar
        :param bucket: Bucket S3 de destino
        :param prefix: Prefijo dentro del bucket
        """
        self.api_url = api_url
        self.bucket = bucket
        self.prefix = prefix

    def fetch_data(self) -> list:
        """Obtiene los datos desde la API"""
        response = requests.get(self.api_url)
        response.raise_for_status()
        print(f"[Ingest] Fetched data from {self.api_url}")
        return response.json()

    def upload_to_s3(self, data: list):
        """Sube los datos a S3 en JSON"""
        s3 = boto3.client("s3")
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
        key = f"{self.prefix}orders_{timestamp}.json"
        s3.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(data))
        print(f"[Ingest] Uploaded data to s3://{self.bucket}/{key}")

    def run(self):
        """Ejecuta el pipeline completo de ingesta"""
        data = self.fetch_data()
        self.upload_to_s3(data)
