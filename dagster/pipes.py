from contextlib import contextmanager
from typing import Any, Iterator, Mapping

from dagster_pipes import PipesDefaultMessageWriter
from httpx import HTTPStatusError
from utils import get_execution_logs, invoke_cloud_function

import dagster._check as check
from dagster import PipesClient  # type: ignore
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.pipes.client import (
    PipesClientCompletedInvocation,
    PipesMessageReader,
    PipesParams,
)
from dagster._core.pipes.context import PipesMessageHandler
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    extract_message_or_forward_to_stdout,
    open_pipes_session,
)


class PipesFunctionLogsMessageReader(PipesMessageReader):
    """Message reader that consumes buffered pipes messages that were flushed on exit from the
    final 4k of logs that are returned from issuing a sync lambda invocation. This means messages
    emitted during the computation will only be processed once the lambda completes.

    Limitations: If the volume of pipes messages exceeds 4k, messages will be lost and it is
    recommended to switch to PipesS3MessageWriter & PipesS3MessageReader.
    """

    @contextmanager
    def read_messages(
        self,
        handler: PipesMessageHandler,
    ) -> Iterator[PipesParams]:
        self._handler = handler
        try:
            # use buffered stdio to shift the pipes messages to the tail of logs
            yield {PipesDefaultMessageWriter.BUFFERED_STDIO_KEY: PipesDefaultMessageWriter.STDERR}
        finally:
            self._handler = None

    def consume_cloud_function_logs(self, response) -> None:
        handler = check.not_none(
            self._handler, "Can only consume logs within context manager scope."
        )

        # Get GCP trace id
        trace_id = response.headers.get("X-Cloud-Trace-Context")
        if trace_id is None:
            trace_id = "troio"
        else:
            trace_id = trace_id.split(";")[0]

        # Get logs
        log_result = get_execution_logs(trace_id)

        for log_line in log_result:
            extract_message_or_forward_to_stdout(handler, log_line)

    def no_messages_debug_text(self) -> str:
        return (
            "Attempted to read messages by extracting them from the tail of lambda logs directly."
        )


class PipesFunctionEventContextInjector(PipesEnvContextInjector):
    def no_messages_debug_text(self) -> str:
        return "Attempted to inject context via the cloud function event input."


class PipesFunctionClient(PipesClient, TreatAsResourceParam):
    """A pipes client for invoking Google Cloud Function.

    By default context is injected via the GCF API call and logs are extracted using google
    cloud logging client.
    """

    def __init__(
        self,
    ):
        self._message_reader = PipesFunctionLogsMessageReader()
        self._context_injector = PipesFunctionEventContextInjector()

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def run(
        self,
        *,
        function_url: str,
        event: Mapping[str, Any],
        context: OpExecutionContext,
    ):
        """Synchronously invoke a lambda function, enriched with the pipes protocol.

        Args:
            function_name (str): The name of the function to use.
            event (Mapping[str, Any]): A JSON serializable object to pass as input to the lambda.
            context (OpExecutionContext): The context of the currently executing Dagster op or asset.
        """
        with open_pipes_session(
            context=context,
            message_reader=self._message_reader,
            context_injector=self._context_injector,
        ) as session:

            if isinstance(self._context_injector, PipesFunctionEventContextInjector):
                payload_data = {
                    **event,
                    **session.get_bootstrap_env_vars(),
                }
            else:
                payload_data: Mapping[str, Any] = event  # type: ignore

            context.log.info(payload_data)

            # Todo: check for errors!
            response = invoke_cloud_function(
                url=function_url,
                data=payload_data,
            )

            context.log.debug(response.status_code)

            if response.status_code != 200:
                context.log.debug(response.content.decode("utf-8"))
                raise HTTPStatusError(
                    f"Failed to invoke cloud function {function_url} with status code {response.status_code}"
                )

            # self._message_reader.consume_cloud_function_logs(response)

        # should probably have a way to return the lambda result payload
        return PipesClientCompletedInvocation(session)
