from prefect.deployments import Deployment
from elt_load_gcs import etl_web_to_gcs
from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer.load("data-pipeline")

docker_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="docker-flow",
    infrastructure=docker_block,
)


if __name__ == "__main__":
    docker_dep.apply()