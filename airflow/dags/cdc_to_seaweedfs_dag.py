from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_cdc_to_seaweedfs():
    import docker

    client = docker.from_env()
    spark_container = client.containers.get("data-platform-spark")

    command = [
        "/opt/spark/bin/spark-submit",
        "--conf", "spark.jars.ivy=/tmp/spark-ivy",
        "--exclude-packages", "software.amazon.awssdk:bundle",
        "--packages",
        (
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,"
            "org.apache.hadoop:hadoop-aws:3.4.2,"
            "software.amazon.awssdk:s3:2.29.52,"
            "software.amazon.awssdk:s3-transfer-manager:2.29.52"
        ),
        "/opt/spark-apps/cdc_to_seaweedfs.py",
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
    dag_id="cdc_to_seaweedfs",
    start_date=datetime(2026, 10, 30),
    schedule=None,
    catchup=False,
    tags=["CDC", "Spark", "SeaweedFS"],
) as dag:

    process_cdc = PythonOperator(
        task_id="process_cdc_events",
        python_callable=run_cdc_to_seaweedfs,
    )