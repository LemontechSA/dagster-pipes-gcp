from pipes import PipesFunctionClient

from dagster import AssetExecutionContext, Definitions, MaterializeResult, asset  # type: ignore


@asset
def cloud_function_pipes_asset(
    context: AssetExecutionContext, pipes_function_client: PipesFunctionClient
) -> MaterializeResult:
    return pipes_function_client.run(
        context=context,
        function_url="http://127.0.0.1:8080",
        event={"some_parameter_value": 1},
    ).get_materialize_result()


defs = Definitions(
    assets=[cloud_function_pipes_asset],
    resources={"pipes_function_client": PipesFunctionClient()},
)
