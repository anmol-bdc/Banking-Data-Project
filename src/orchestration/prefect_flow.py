from __future__ import annotations

from prefect import flow, task, get_run_logger


@task(name="run-transformation-pipeline")
def run_pipeline():
    """Run the transformation pipeline as a Prefect task."""

    logger = get_run_logger()

    # Import the transformation entry point so the pipeline logic is executed
    # from the same module structure used by the rest of the project.
    logger.info("Importing the transformation runner module")
    from src.transformation.transformation_runner import main

    # Start the Spark-based transformation flow after the runner module is loaded.
    logger.info("Starting the transformation pipeline")
    main()

    # Report the completion of the transformation flow once all stages finish.
    logger.info("Transformation pipeline completed successfully")


@flow(
    name="banking-data-factory-flow",
    log_prints=True
)
def banking_data_factory_flow():
    """Coordinate the full Prefect workflow for the banking data pipeline."""

    logger = get_run_logger()

    # Announce the beginning of the workflow so orchestration logs clearly show
    # when the batch processing run starts.
    logger.info("Prefect flow started")

    # Execute the transformation task as the core processing step of this flow.
    run_pipeline()

    # Log the successful completion of the workflow after the transformation task
    # has finished without raising an exception.
    logger.info("Prefect flow completed")


if __name__ == "__main__":
    # Run the flow directly when this module is executed as a script.
    print("Starting Prefect flow")
    banking_data_factory_flow()
    print("Prefect flow finished")