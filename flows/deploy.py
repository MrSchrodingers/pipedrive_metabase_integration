from datetime import timedelta
from prefect.schedules import Interval
from flows.pipedrive_metabase_etl import main_etl_flow
from prefect_kubernetes.jobs import KubernetesJob

if __name__ == "__main__":
    deployment = main_etl_flow.to_deployment(
        name="pipedrive_etl_deployment",
        version="1.1",
        tags=["pipedrive", "etl", "database"],
        parameters={"run_batch_size": 1500},
        schedule=Interval(
            timedelta(minutes=30),
        ),
        infrastructure=KubernetesJob(
            image="pipedrive_metabase_integration-etl:latest",
            image_pull_policy="Never",
            env={"PREFECT_API_URL": "http://prefect-orion:4200/api"}, 
            env_from=[
                {
                    "secretRef": {
                        "name": "app-secrets" 
                    }
                },
                {
                    "secretRef": {
                        "name": "db-secrets"
                    }
                }
            ]
        )
    )

    # serve(deployment)
