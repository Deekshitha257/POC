from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import shlex
import os
import logging
import shutil
import traceback

SPARK_JOB = "/opt/airflow/spark_jobs/mongo_pyspark.py"
JAR_DIR = "/opt/airflow/jars"
JARS = [
    os.path.join(JAR_DIR, "mongo-spark-connector_2.12-10.1.1.jar"),
    os.path.join(JAR_DIR, "bson-4.11.2.jar"),
    os.path.join(JAR_DIR, "mongodb-driver-core-4.11.2.jar"),
    os.path.join(JAR_DIR, "mongodb-driver-sync-4.11.2.jar"),
    os.path.join(JAR_DIR, "iceberg-spark-runtime-3.5_2.12-1.5.2.jar"),
    os.path.join(JAR_DIR, "iceberg-nessie-1.5.2.jar"),
    os.path.join(JAR_DIR, "nessie-client-0.99.0.jar"),
    os.path.join(JAR_DIR, "nessie-spark-extensions-3.4_2.12-0.105.7.jar"),
    os.path.join(JAR_DIR, "hadoop-aws-3.3.4.jar"),
    os.path.join(JAR_DIR, "aws-java-sdk-bundle-1.12.772.jar"),
]

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

def run_spark_job(**context):
    logger.info("START run_spark_job")
    for path in [SPARK_JOB, JAR_DIR]:
        logger.info("Checking path: %s exists=%s", path, os.path.exists(path))

    for jar in JARS:
        exists = os.path.exists(jar)
        size = os.path.getsize(jar) if exists else None
        logger.info("Jar check: %s exists=%s size=%s", jar, exists, size)

    spark_submit_path = shutil.which("spark-submit")
    logger.info("which spark-submit -> %s", spark_submit_path)

    jars_arg = ",".join(JARS)
    cmd = f"spark-submit --jars {jars_arg} {SPARK_JOB}"

    env = os.environ.copy()
    env["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    env["SPARK_HOME"] = "/opt/spark/spark-3.4.1-bin-hadoop3"
    env["PATH"] = f"{env['SPARK_HOME']}/bin:{env['JAVA_HOME']}/bin:{env.get('PATH','')}"

    logger.info("ENV SPARK_HOME=%s JAVA_HOME=%s", env.get("SPARK_HOME"), env.get("JAVA_HOME"))
    logger.info("Env PATH head: %s", env["PATH"].split(":")[:5])

    try:
        logger.info("Running command: %s", cmd)
        proc = subprocess.run(shlex.split(cmd), env=env, capture_output=True, text=True)
        logger.info("Process finished. returncode=%s", proc.returncode)

        if proc.stdout:
            for line in proc.stdout.splitlines():
                logger.info("spark-submit stdout | %s", line)

        if proc.stderr:
            for line in proc.stderr.splitlines():
                logger.error("spark-submit stderr | %s", line)

        if proc.returncode != 0:
            raise RuntimeError(f"spark-submit failed with exit {proc.returncode}")

        logger.info("END run_spark_job SUCCESS")
    except Exception as e:
        tb = traceback.format_exc()
        logger.error("Exception in run_spark_job: %s", str(e))
        logger.error("Traceback:\n%s", tb)
        raise

default_args = {
    "owner": "deeku",
    "depends_on_past": False,
}

with DAG(
    dag_id="mongo_iceberg_dag1",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["mongo", "iceberg", "spark"],
) as dag:
    run = PythonOperator(
        task_id="run_mongo_to_iceberg",
        python_callable=run_spark_job,
        provide_context=True,
    )




