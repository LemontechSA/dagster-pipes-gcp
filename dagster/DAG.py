from dg_pipes import PipesCloudFunctionClient

from dagster import AssetExecutionContext, Definitions, MaterializeResult, asset  # type: ignore


@asset(
    description="A cloud function that writes fake data to a delta table.",
)
def cloud_function_pipes_asset(
    context: AssetExecutionContext, pipes_function_client: PipesCloudFunctionClient
) -> MaterializeResult:
    return pipes_function_client.run(
        context=context,
        # function_url="http://127.0.0.1:8080",
        function_url="https://europe-west4-jasper-ginn-dagster.cloudfunctions.net/dagster-pipes-gcp-nprod",
        event={"table_location": "gs://dala-cst-euw4-jgdag-prd/bronze/fake_data"},
        env={
            "bucket": "dala-cst-euw4-jgdag-prd",
        },
    ).get_materialize_result()


defs = Definitions(
    assets=[cloud_function_pipes_asset],
    resources={"pipes_function_client": PipesCloudFunctionClient()},
)
