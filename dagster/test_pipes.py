from multiprocessing import Process
from unittest import mock

import pytest
import uvicorn
from dg_pipes import PipesCloudFunctionClient, PipesCloudStorageMessageReader
from fastapi import FastAPI

from dagster import AssetsDefinition, asset  # type: ignore

app = FastAPI()


@app.post("/fn")
async def endpoint():
    return 200, "OK"


def run_server():
    uvicorn.run(app, port=9200)


@pytest.fixture()
def httpserver():
    p = Process(target=run_server, args=(), daemon=True)
    p.start()
    yield p
    p.terminate()


@pytest.fixture(params=[True, False])
def mock_storage_client(request):
    mock_client = mock.MagicMock()
    mock_bucket = mock.MagicMock()
    mock_blob = mock.MagicMock()
    mock_blob.exists.return_value = request.param
    mock_blob.download_as_text.return_value = "some message"
    mock_client.bucket.return_value = mock_bucket
    mock_client.bucket.return_value.blob.return_value = mock_blob
    return mock_client


@pytest.fixture()
def test_asset() -> AssetsDefinition:
    @asset
    def test_asset(context):
        return "test"

    return test_asset


def test_pipes_cloud_storage_message_reader(mock_storage_client):
    reader = PipesCloudStorageMessageReader(client=mock_storage_client, bucket="test_bucket")
    msg = reader.download_messages_chunk(0, params={"key_prefix": "test_key_prefix"})
    if mock_storage_client.bucket().blob().exists():
        assert msg == "some message"  # nosec
    else:
        assert msg is None  # nosec


def test_pipes_cloud_function_client(httpserver, test_asset: AssetsDefinition):
    client = PipesCloudFunctionClient(
        message_reader=PipesCloudStorageMessageReader(
            client=mock.MagicMock(), bucket="test_bucket"
        ),
    )
    with mock.patch("dg_pipes.open_pipes_session") as _:
        context = mock.MagicMock()
        client.run(
            function_url="http://localhost:9200/fn",
            context=context,
            event={"some_input": "some_value"},
        )
