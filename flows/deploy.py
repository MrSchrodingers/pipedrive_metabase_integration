from flows.pipedrive_metabase_etl import main_flow

if __name__ == "__main__":
    deployment = main_flow.deploy(
        name="pipedrive-metabase-etl-deployment",
        work_pool_name="docker-custom",
        image="pipedrive_metabase_integration:latest",
        job_variables={"image_pull_policy": "Never"}
    )
    print("Deployment aplicado:", deployment)
