import io
from unittest import mock

from fn_pipes import PipesCloudStorageMessageWriter, PipesCloudStorageMessageWriterChannel


def test_pipes_cloud_storage_message_writer():
    mock_client = mock.MagicMock()
    writer = PipesCloudStorageMessageWriter(client=mock_client)
    with mock.patch("fn_pipes._assert_env_param_type") as mock_assert_env_param_type:
        with mock.patch("fn_pipes._assert_opt_env_param_type") as mock_assert_opt_env_param_type:
            mock_assert_env_param_type.return_value = "test_bucket_val"
            mock_assert_opt_env_param_type.return_value = "test_key_prefix_val"
            channel = writer.make_channel(params={})
            mock_assert_env_param_type.assert_called_once()
            mock_assert_opt_env_param_type.assert_called_once()
    assert isinstance(channel, PipesCloudStorageMessageWriterChannel)  # nosec


def test_pipes_cloud_storage_message_writer_channel():
    mock_client = mock.MagicMock()
    mock_bucket = mock.MagicMock()
    mock_blob = mock.MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_client.bucket.return_value.blob.return_value = mock_blob
    payload = io.StringIO("some message")
    channel = PipesCloudStorageMessageWriterChannel(
        client=mock_client,
        bucket="test_bucket",
        key_prefix="test_key_prefix",
    )
    channel.upload_messages_chunk(payload, 0)
    mock_bucket.blob.assert_called_once_with("test_key_prefix/0.json")
    mock_blob.upload_from_string.assert_called_once_with("some message")
