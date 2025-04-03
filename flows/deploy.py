from datetime import timedelta
from prefect import serve
from prefect.schedules import Interval
from flows.pipedrive_metabase_etl import main_etl_flow

if __name__ == "__main__":
    deployment = main_etl_flow.to_deployment(
        name="pipedrive_etl_deployment",
        version="1.1",
        tags=["pipedrive", "etl", "database"],
        parameters={"run_batch_size": 1500},
        schedule=Interval(
            timedelta(hours=12),
        )
    )

    serve(deployment)
