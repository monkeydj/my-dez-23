from prefect.deployments import Deployment
from elt_web_to_gcs import elt_batch, etl_web_to_gcs

deployment = Deployment.build_from_flow(
    flow=elt_batch,
    name="elt-data-warehouse-hw",
    tags=["homework", "wk3"],
    parameters={"color": "fhv", "year": 2019},
)

deployment_single = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="etl-upload-gzipped-csv-hw",
    tags=["homework", "wk3"],
    parameters={"color": "fhv", "year": 2019, "month": 1},
)

if __name__ == "__main__":
    deployment.apply()
    deployment_single.apply()
