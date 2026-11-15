from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_spark_job():
    import docker

    client = docker.from_env()

    spark_container = client.containers.get("data-platform-spark")

    command = [
        "/opt/spark/bin/spark-submit",
        "--conf", "spark.jars.ivy=/tmp/spark-ivy",
        "--conf", "spark.driver.host=127.0.0.1",
        "--conf", "spark.driver.bindAddress=127.0.0.1",
        "--exclude-packages", "software.amazon.awssdk:bundle",
        "--packages",
        (
            "org.apache.hadoop:hadoop-aws:3.4.2,"
            "software.amazon.awssdk:s3:2.29.52,"
            "software.amazon.awssdk:s3-transfer-manager:2.29.52"
        ),
        "/opt/spark-apps/test_job.py",
    ]

    result = spark_container.exec_run(
        command,
        demux=True,
        environment={
            "SPARK_LOCAL_HOSTNAME": "localhost",
            "SPARK_LOCAL_IP": "127.0.0.1",
        },
    )

    stdout, stderr = result.output

    if stdout:
        print(stdout.decode("utf-8", errors="replace"))

    if stderr:
        print(stderr.decode("utf-8", errors="replace"))

    if result.exit_code != 0:
        raise RuntimeError(
            f"Spark завершился с кодом {result.exit_code}"
        )


with DAG(
    dag_id="spark_test_to_seaweedfs",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "seaweedfs"],
) as dag:

    run_job = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job,
    )