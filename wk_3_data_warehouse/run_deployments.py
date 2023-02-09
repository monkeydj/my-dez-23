from prefect.deployments import run_deployment

"""A naive attempt to run multiple flows concurrently."""

deployment_name = "etl-web-to-gcs/etl-upload-gzipped-csv-hw"


def run_with_params(**flow_params):
    print(f"Trigger flow run params={flow_params}")
    run_deployment(name=deployment_name, parameters=flow_params)


def main():
    color, year, months = ("fhv", 2019, list(range(2, 13)))
    for month in months:
        run_with_params(color=color, year=year, month=month)


if __name__ == "__main__":
    main()
