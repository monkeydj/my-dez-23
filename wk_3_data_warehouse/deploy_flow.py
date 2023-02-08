from prefect.deployments import Deployment
from elt_web_to_gcs import elt_batch

deployment = Deployment.build_from_flow(
    flow=elt_batch,
    name="elt-data-warehouse-hw",
    tags=["homework", "wk3"],
    parameters={"color": "fhv", "year": 2019},
    skip_upload=True,
)


if __name__ == "__main__":
    deployment.apply()
