# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import subprocess
# import shlex
# import os
# import logging
# import shutil
# import traceback

# SPARK_JOB = "/opt/airflow/spark_jobs/mongo_pyspark.py"
# JAR_DIR = "/opt/airflow/jars"
# JARS = [
#     os.path.join(JAR_DIR, "mongo-spark-connector_2.12-10.1.1.jar"),
#     os.path.join(JAR_DIR, "bson-4.11.2.jar"),
#     os.path.join(JAR_DIR, "mongodb-driver-core-4.11.2.jar"),
#     os.path.join(JAR_DIR, "mongodb-driver-sync-4.11.2.jar"),
#     os.path.join(JAR_DIR, "iceberg-spark-runtime-3.5_2.12-1.5.2.jar"),
#     os.path.join(JAR_DIR, "iceberg-nessie-1.5.2.jar"),
#     os.path.join(JAR_DIR, "nessie-client-0.99.0.jar"),
#     os.path.join(JAR_DIR, "nessie-spark-extensions-3.4_2.12-0.105.7.jar"),
#     os.path.join(JAR_DIR, "hadoop-aws-3.3.4.jar"),
#     os.path.join(JAR_DIR, "aws-java-sdk-bundle-1.12.772.jar"),
# ]

# logger = logging.getLogger("airflow.task")
# logger.setLevel(logging.INFO)

# def run_spark_job(**context):
#     logger.info("START run_spark_job")
#     for path in [SPARK_JOB, JAR_DIR]:
#         logger.info("Checking path: %s exists=%s", path, os.path.exists(path))

#     for jar in JARS:
#         exists = os.path.exists(jar)
#         size = os.path.getsize(jar) if exists else None
#         logger.info("Jar check: %s exists=%s size=%s", jar, exists, size)

#     spark_submit_path = shutil.which("spark-submit")
#     logger.info("which spark-submit -> %s", spark_submit_path)

#     jars_arg = ",".join(JARS)
#     cmd = f"spark-submit --jars {jars_arg} {SPARK_JOB}"

#     env = os.environ.copy()
#     env["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
#     env["SPARK_HOME"] = "/opt/spark/spark-3.4.1-bin-hadoop3"
#     env["PATH"] = f"{env['SPARK_HOME']}/bin:{env['JAVA_HOME']}/bin:{env.get('PATH','')}"

#     logger.info("ENV SPARK_HOME=%s JAVA_HOME=%s", env.get("SPARK_HOME"), env.get("JAVA_HOME"))
#     logger.info("Env PATH head: %s", env["PATH"].split(":")[:5])

#     try:
#         logger.info("Running command: %s", cmd)
#         proc = subprocess.run(shlex.split(cmd), env=env, capture_output=True, text=True)
#         logger.info("Process finished. returncode=%s", proc.returncode)

#         if proc.stdout:
#             for line in proc.stdout.splitlines():
#                 logger.info("spark-submit stdout | %s", line)

#         if proc.stderr:
#             for line in proc.stderr.splitlines():
#                 logger.error("spark-submit stderr | %s", line)

#         if proc.returncode != 0:
#             raise RuntimeError(f"spark-submit failed with exit {proc.returncode}")

#         logger.info("END run_spark_job SUCCESS")
#     except Exception as e:
#         tb = traceback.format_exc()
#         logger.error("Exception in run_spark_job: %s", str(e))
#         logger.error("Traceback:\n%s", tb)
#         raise

# default_args = {
#     "owner": "deeku",
#     "depends_on_past": False,
# }

# with DAG(
#     dag_id="mongo_iceberg_dag1",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     default_args=default_args,
#     tags=["mongo", "iceberg", "spark"],
# ) as dag:
#     run = PythonOperator(
#         task_id="run_mongo_to_iceberg",
#         python_callable=run_spark_job,
#         provide_context=True,
#     )


# --------------------------------------------------------------------------------------
import os
import sys
import time
import json
import logging
import traceback
import shlex
import shutil
import subprocess
from datetime import datetime
from urllib.parse import urlparse

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


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

DREMIO_HOST = os.getenv("DREMIO_HOST", "dremio")
DREMIO_PORT = os.getenv("DREMIO_PORT", "9047")
DREMIO_USER = os.getenv("DREMIO_USER", "deekshitha")
DREMIO_PASS = os.getenv("DREMIO_PASS", "deekshitha257")
DREMIO_BASE = f"http://{DREMIO_HOST}:{DREMIO_PORT}"
DREMIO_SOURCE_NAME = os.getenv("DREMIO_SOURCE_NAME", "iceberg")

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
S3_PATH_STYLE = os.getenv("S3_PATH_STYLE", "true")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")
NESSIE_REF = os.getenv("NESSIE_REF", "main")

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
    env["PATH"] = f"{env['SPARK_HOME']}/bin:{env['JAVA_HOME']}/bin:{env.get('PATH', '')}"

    logger.info("ENV SPARK_HOME=%s JAVA_HOME=%s",
                env.get("SPARK_HOME"), env.get("JAVA_HOME"))
    logger.info("Env PATH head: %s", env["PATH"].split(":")[:5])

    try:
        logger.info("Running command: %s", cmd)
        proc = subprocess.run(shlex.split(cmd), env=env,
                              capture_output=True, text=True)
        logger.info("Process finished. returncode=%s", proc.returncode)

        if proc.stdout:
            for line in proc.stdout.splitlines():
                logger.info("spark-submit stdout | %s", line)

        if proc.stderr:
            for line in proc.stderr.splitlines():
                logger.error("spark-submit stderr | %s", line)

        if proc.returncode != 0:
            raise RuntimeError(
                f"spark-submit failed with exit {proc.returncode}")

        logger.info("END run_spark_job SUCCESS")
    except Exception as e:
        tb = traceback.format_exc()
        logger.error("Exception in run_spark_job: %s", str(e))
        logger.error("Traceback:\n%s", tb)
        raise




def dremio_login_token():
    url = f"{DREMIO_BASE}/apiv2/login"
    payload = {
        "userName": DREMIO_USER,
        "password": DREMIO_PASS
    }
    r = requests.post(url, json=payload)
    if r.status_code != 200:
        raise Exception(f"Dremio login failed: {r.status_code} {r.text}")
    return r.json()["token"]


def dremio_headers():
    token = dremio_login_token()
    return {
        "Content-Type": "application/json",
        "Authorization": f"_dremio{token}"
    }


def dremio_list_sources():
    url = f"{DREMIO_BASE}/api/v3/source"
    r = requests.get(url, headers=dremio_headers())
    r.raise_for_status()
    return r.json()


def dremio_source_exists(name: str) -> bool:
    try:
        sources = dremio_list_sources()
        for s in sources:
            if s.get("name") == name:
                return True
        return False
    except Exception:
        return False


def create_dremio_iceberg_source(**context):

    logger.info("create_dremio_iceberg_source: start")

    minio_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    minio_access = os.getenv("S3_ACCESS_KEY", "minioadmin")
    minio_secret = os.getenv("S3_SECRET_KEY", "minioadmin123")
    iceberg_wh = os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg")
    s3_path_style = os.getenv("S3_PATH_STYLE", "true")
    nessie_raw = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")
    nessie_ref = os.getenv("NESSIE_REF", "main")

    iceberg_wh = iceberg_wh.rstrip("/")

    api_base = f"{DREMIO_BASE.rstrip('/')}/api/v3/source"
    src_name = os.getenv("DREMIO_SOURCE_NAME", "iceberg")

    src_exists = dremio_source_exists(src_name)
    src_id = None
    if src_exists:
        try:
            sources = dremio_list_sources()
            for s in sources:
                if s.get("name") == src_name:
                    src_id = s.get("id") or s.get(
                        "entityId") or s.get("config", {}).get("id")
                    break
        except Exception as e:
            logger.debug("Could not list sources: %s", e)

    logger.info("Dremio source exists=%s id=%s", src_exists, src_id)

    try:
        headers = dremio_headers()
    except Exception as ex:
        logger.exception("Failed to obtain Dremio token/headers: %s", ex)
        raise RuntimeError(f"Failed to obtain Dremio token: {ex}")

    logger.info("Checking available source types...")
    try:
        type_url = f"{DREMIO_BASE}/api/v3/source/type"
        resp = requests.get(type_url, headers=headers, timeout=30)
        if resp.status_code == 200:
            source_types = resp.json()
            logger.info("Available source types: %s",
                        json.dumps(source_types, indent=2))
        else:
            logger.warning("Could not get source types: %s", resp.status_code)
    except Exception as e:
        logger.warning("Error checking source types: %s", e)

    attempts = []

    uri_formats = [
        "http://nessie:19120/api/v2",
        "http://nessie:19120/api/v1",
        "http://nessie:19120",
        "nessie://nessie:19120/api/v2",
        "nessie://nessie:19120/api/v1",
        "nessie://nessie:19120",
    ]

    for uri in uri_formats:
        attempts.append({
            "type": "NESSIE",
            "config": {
                "nessieUri": uri,
                "defaultBranch": nessie_ref,
                "warehouse": iceberg_wh,
                "propertyList": [
                    {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                    {"name": "fs.s3a.access.key", "value": minio_access},
                    {"name": "fs.s3a.secret.key", "value": minio_secret},
                    {"name": "fs.s3a.path.style.access", "value": s3_path_style},
                    {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
                    {"name": "fs.s3a.endpoint.region", "value": "us-east-1"}
                ]
            }
        })

    attempts.append({
        "type": "NESSIE",
        "config": {
            "defaultBranch": nessie_ref,
            "warehouse": iceberg_wh,
            "propertyList": [
                {"name": "nessie.uri", "value": "http://nessie:19120/api/v2"},
                {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                {"name": "fs.s3a.access.key", "value": minio_access},
                {"name": "fs.s3a.secret.key", "value": minio_secret},
                {"name": "fs.s3a.path.style.access", "value": s3_path_style},
                {"name": "fs.s3a.connection.ssl.enabled", "value": "false"}
            ]
        }
    })

    attempts.append({
        "type": "ARCTIC",
        "config": {
            "nessieUri": "http://nessie:19120/api/v2",
            "defaultBranch": nessie_ref,
            "warehouse": iceberg_wh,
            "propertyList": [
                {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                {"name": "fs.s3a.access.key", "value": minio_access},
                {"name": "fs.s3a.secret.key", "value": minio_secret},
                {"name": "fs.s3a.path.style.access", "value": s3_path_style},
                {"name": "fs.s3a.connection.ssl.enabled", "value": "false"}
            ]
        }
    })

    attempts.append({
        "type": "DATA_LAKE",
        "config": {
            "rootPath": iceberg_wh,
            "propertyList": [
                {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                {"name": "fs.s3a.access.key", "value": minio_access},
                {"name": "fs.s3a.secret.key", "value": minio_secret},
                {"name": "fs.s3a.path.style.access", "value": s3_path_style},
                {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
                {"name": "iceberg.catalog.type", "value": "nessie"},
                {"name": "iceberg.catalog.uri",
                    "value": "http://nessie:19120/api/v2"},
                {"name": "iceberg.catalog.ref", "value": nessie_ref}
            ]
        }
    })

    attempts.append({
        "type": "HIVE",
        "config": {
            "propertyList": [
                {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                {"name": "fs.s3a.access.key", "value": minio_access},
                {"name": "fs.s3a.secret.key", "value": minio_secret},
                {"name": "fs.s3a.path.style.access", "value": s3_path_style},
                {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
                {"name": "iceberg.enabled", "value": "true"},
                {"name": "iceberg.catalog.type", "value": "nessie"},
                {"name": "iceberg.catalog.uri",
                    "value": "http://nessie:19120/api/v2"},
                {"name": "iceberg.catalog.warehouse", "value": iceberg_wh},
                {"name": "iceberg.catalog.ref", "value": nessie_ref}
            ]
        }
    })

    for idx, attempt in enumerate(attempts):
        logger.info("=== Trying approach %d/%d: type=%s ===",
                    idx + 1, len(attempts), attempt["type"])

        payload = {
            "entityType": "source",
            "name": src_name,
            "type": attempt["type"],
            "config": attempt["config"]
        }

        safe_config = attempt["config"].copy()
        if "propertyList" in safe_config:
            for prop in safe_config["propertyList"]:
                if "secret" in prop.get("name", "").lower() or "key" in prop.get("name", "").lower():
                    prop["value"] = "***MASKED***"
        logger.info("Attempt config (masked): %s",
                    json.dumps(safe_config, indent=2))

        try:
            if src_id:
                url = f"{api_base}/{src_id}"
                logger.info("Updating existing source")
                resp = requests.put(url, json=payload,
                                    headers=headers, timeout=30)
            else:
                logger.info("Creating new source")
                resp = requests.post(api_base, json=payload,
                                     headers=headers, timeout=30)

            logger.info("Response status: %s", resp.status_code)

            if resp.status_code in (200, 201):
                logger.info(
                    "SUCCESS! Created source with approach %d", idx + 1)
                result = resp.json() if resp.text else {}
                if context and "ti" in context:
                    try:
                        context["ti"].xcom_push(
                            key="dremio_source_create", value=result)
                        context["ti"].xcom_push(
                            key="approach_used", value=idx + 1)
                        context["ti"].xcom_push(
                            key="source_type", value=attempt["type"])
                    except Exception:
                        logger.debug("XCom push failed.")
                return result

            if resp.status_code == 409:
                logger.warning("Source already exists")
                return {"status": "exists", "message": "Source already exists"}

            error_info = resp.text[:500] if resp.text else "<no response body>"
            logger.info("Response error: %s", error_info)

            if resp.status_code == 400:
                try:
                    error_json = resp.json()
                    logger.info("Validation error details: %s",
                                json.dumps(error_json, indent=2))
                except:
                    pass

        except requests.exceptions.RequestException as ex:
            logger.warning("Request failed: %s", ex)
            continue

    logger.error("All %d approaches failed", len(attempts))

    try:
        schema_url = f"{DREMIO_BASE}/api/v3/source/configuration/NESSIE"
        resp = requests.get(schema_url, headers=headers, timeout=30)
        if resp.status_code == 200:
            logger.info("NESSIE configuration schema: %s",
                        json.dumps(resp.json(), indent=2)[:1000])
    except Exception as e:
        logger.warning("Could not get NESSIE schema: %s", e)

    raise RuntimeError(f"Failed to create Dremio source after {len(attempts)} attempts. " +
                       "Check Dremio documentation for correct source configuration.")


def run_dremio_sql(sql: str, timeout_seconds: int = 300):
   
    try:
        headers = dremio_headers()
        submit_url = f"{DREMIO_BASE}/api/v3/sql"
        logger.info("Submitting query to Dremio: %s", sql[:200])

        resp = requests.post(submit_url, json={"sql": sql}, headers=headers, timeout=30)
        resp.raise_for_status()
        job = resp.json()
        job_id = job.get("id") or job.get("jobId")
        if not job_id:
            raise RuntimeError(f"Could not get job id from Dremio response: {job}")
        logger.info("Dremio job started: %s", job_id)

        poll_url = f"{DREMIO_BASE}/api/v3/job/{job_id}"
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            status_resp = requests.get(poll_url, headers=headers, timeout=30)
            status_resp.raise_for_status()
            job_status = status_resp.json()

            logger.debug("Job status JSON: %s", json.dumps(job_status)[:2000])

            state = job_status.get("state")
            if state == "COMPLETED":
                results_url = f"{DREMIO_BASE}/api/v3/job/{job_id}/results"
                results_resp = requests.get(results_url, headers=headers, timeout=60)
                results_resp.raise_for_status()
                logger.info("Query completed successfully. Job: %s", job_id)
                return results_resp.json()

            if state in ("FAILED", "CANCELED", "CANCELLED"):
                error_msg = job_status.get("errorMessage") or job_status.get("error", {})
                raise RuntimeError(f"Dremio job failed: {error_msg}")

            if int(time.time() - start_time) % 30 == 0:
                try:
                    profile_resp = requests.get(f"{poll_url}/profile", headers=headers, timeout=30)
                    logger.debug("Job profile (partial): %s", (profile_resp.text or "")[:2000])
                except Exception:
                    pass

            time.sleep(2)

        raise TimeoutError(f"Timed out waiting for Dremio job {job_id} to finish after {timeout_seconds} seconds")

    except requests.exceptions.RequestException as e:
        logger.error("HTTP error executing Dremio query: %s", e)
        raise
    except Exception as e:
        logger.error("Error executing Dremio query: %s", e)
        raise


def query_dremio_tables(**context):
   
    source_name = os.getenv("DREMIO_SOURCE_NAME", "iceberg")

    logger.info("=== Querying Dremio Tables ===")

    sql_queries = [
        f'SHOW SCHEMAS IN "{source_name}"',

        f'SHOW TABLES IN "{source_name}"',

        f'SHOW TABLES IN "{source_name}"."ai_segmentation"',

        f'SELECT COUNT(*) as row_count FROM "{source_name}"."ai_segmentation"."conversations"',

        f'SELECT * FROM "{source_name}"."ai_segmentation"."conversations" LIMIT 5',
    ]

    all_results = []

    for sql in sql_queries:
        logger.info("Executing query: %s", sql)
        try:
            result = run_dremio_sql(sql, timeout_seconds=30)
            logger.info("Query successful!")

            if isinstance(result, dict):
                if 'rows' in result:
                    rows = result['rows']
                    logger.info("Number of rows returned: %d", len(rows))
                    if rows:
                        logger.info("First few rows: %s",
                                    json.dumps(rows[:3], indent=2))
                else:
                    logger.info("Result structure: %s",
                                json.dumps(result, indent=2)[:500])

            all_results.append({
                "query": sql,
                "success": True,
                "result": result
            })

            if "SELECT" in sql.upper() and "COUNT" in sql.upper():
                logger.info("Got count query result, returning...")
                if context and 'ti' in context:
                    context['ti'].xcom_push(
                        key="dremio_query_results", value=all_results)
                return all_results[-1]  

        except TimeoutError as e:
            logger.warning("Query timed out: %s", sql)
            all_results.append({
                "query": sql,
                "success": False,
                "error": "Timeout",
                "message": str(e)
            })
        except Exception as e:
            logger.warning("Query failed: %s - Error: %s", sql, str(e))
            all_results.append({
                "query": sql,
                "success": False,
                "error": str(e)
            })

    logger.info("=== All Query Attempts Summary ===")
    for i, res in enumerate(all_results):
        status = "✓" if res.get("success") else "✗"
        logger.info("[%s] %d. %s", status, i+1, res['query'])
        if not res.get("success"):
            logger.info("   Error: %s", res.get('error', 'Unknown'))

    if context and 'ti' in context:
        context['ti'].xcom_push(key="dremio_query_results", value=all_results)

    return {
        "status": "partial_success",
        "total_queries": len(sql_queries),
        "successful": len([r for r in all_results if r.get("success")]),
        "failed": len([r for r in all_results if not r.get("success")]),
        "results": all_results
    }


default_args = {
    "owner": "deeku",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="mongo_iceberg_dremio_dagg",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["mongo", "iceberg", "spark", "dremio"],
) as dag:

    run = PythonOperator(
        task_id="run_mongo_to_iceberg",
        python_callable=run_spark_job,
        provide_context=True,
    )

    configure_dremio = PythonOperator(
        task_id="configure_dremio_source",
        python_callable=create_dremio_iceberg_source,
        provide_context=True,
    )

    dremio_check = PythonOperator(
        task_id="query_dremio_tables",
        python_callable=query_dremio_tables,
        provide_context=True,
    )

    run >> configure_dremio >> dremio_check
