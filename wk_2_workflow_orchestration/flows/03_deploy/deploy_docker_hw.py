from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from etl_parameterized_hw_q3 import elt_wrapped_flow

docker_block = DockerContainer.load("dez-docker")

docker_dep = Deployment.build_from_flow(
    flow=elt_wrapped_flow,
    name="docker-flow-direct-web-to-bq",
    parameters={"color": "yellow", "year": 2019, "months": [2, 3]},
    tags=["homework", "hw2/q3"],
    infrastructure=docker_block,
)


if __name__ == "__main__":
    docker_dep.apply()
