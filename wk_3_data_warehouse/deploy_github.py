from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from elt_web_to_gcs import elt_batch

github_block = GitHub.load("dez-github")

deployment = Deployment.build_from_flow(
    flow=elt_batch,
    name="elt-data-warehouse-github",
    storage_block=github_block,
    tags=["homework", "wk3"],
    parameters={"color": "fhv", "year": 2019}
)


if __name__ == "__main__":
    deployment.apply()
