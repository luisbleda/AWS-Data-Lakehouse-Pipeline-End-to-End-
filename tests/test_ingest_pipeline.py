import pytest
from unittest.mock import patch, MagicMock
from src.ingestion.ingest_pipeline import IngestPipeline


@pytest.fixture
def ingest():
    return IngestPipeline(
        api_url="https://fakestoreapi.com/orders",
        bucket="test-bucket",
        prefix="raw/orders/"
    )


def test_fetch_data(ingest):
    with patch("requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"id": 1, "products": []}]
        mock_resp.raise_for_status = lambda: None
        mock_get.return_value = mock_resp

        data = ingest.fetch_data()
        assert isinstance(data, list)
        assert data[0]["id"] == 1


def test_upload_to_s3(ingest):
    with patch("boto3.client") as mock_boto:
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        ingest.upload_to_s3([{"id": 1}])
        mock_s3.put_object.assert_called_once()
