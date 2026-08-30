from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
import pendulum

@dag(
        dag_id="orchestrate",
        schedule=("0 11 * * *"), 
        catchup=False
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
)
def orchestrate():

    @task
    def ingest_cdc():
        ws = WorkspaceClient(
        host="_host",
        token="databricks_token"
        )

        job_trigger = ws.jobs.run_now(job_id="your_databricks_job_id")

        while True:

            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break 
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
                    
            time.sleep(5)  # Wait for 5 seconds before checking the job status again
        return "CDC ingestion completed"

    @task.bash
    def clean_target():
        return "rm -rf opt/airflow/walmart_project/target && rm -rf opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory using the "cd" command
        return "rm -rf opt/airflow/walmart_project/target && cd opt/airflow/walmart_project && dbt source freshness"

    @task.bash
    def silver_technical():
        return "cd opt/airflow/walmart_project && dbt run --select silver_t"

    @task.bash
    def silver_technical_test():
        return "cd opt/airflow/walmart_project && dbt test --select silver_b"

    @task.bash
    def silver_business_tests():
        return "cd opt/airflow/walmart_project && dbt test --select silver_t"

    @task.bash
    def gold_ephemeral():
        return "cd opt/airflow/walmart_project && dbt run --select gold/ephemeral"

    @task.bash
    def gold_dimensions():
        return "cd opt/airflow/walmart_project && dbt snapshot"

    @task.bash
    def gold_facts():
        return "cd opt/airflow/walmart_project && dbt run --select gold/facts"

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical() >> silver_technical_test() >> silver_business_tests() >> gold_ephemeral() >> gold_dimensions() >> gold_facts()
orchestrate_dag = orchestrate()
