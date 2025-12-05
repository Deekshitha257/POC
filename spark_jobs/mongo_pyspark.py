import os
import sys
import time
import logging
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("mongo_to_iceberg")

MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASS = os.getenv("MONGO_PASS", "example")
MONGO_DB = os.getenv("MONGO_DB", "ai_segmentation")
MONGO_COLL = os.getenv("MONGO_COLL", "conversations")
AUTH_SOURCE = os.getenv("MONGO_AUTHSOURCE", "admin")

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

S3_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg/")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
S3_PATH_STYLE = os.getenv("S3_PATH_STYLE", "true")
S3_SSL = os.getenv("S3_SSL", "false")


NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
NESSIE_REF = os.getenv("NESSIE_REF", "main")


def nowts():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


def build_mongo_uri():
    if MONGO_USER == "" or MONGO_PASS == "":
        return "mongodb://" + MONGO_HOST + ":" + MONGO_PORT + "/" + MONGO_DB + "." + MONGO_COLL
    else:
        return "mongodb://" + MONGO_USER + ":" + MONGO_PASS + "@" + MONGO_HOST + ":" + MONGO_PORT + "/" + MONGO_DB + "." + MONGO_COLL + "?authSource=" + AUTH_SOURCE


def check_jars():
    missing = []
    logger.info("Checking required JARs under %s", JAR_DIR)
    for j in JARS:
        exists = os.path.exists(j)
        size = None
        try:
            if exists:
                size = os.path.getsize(j)
        except Exception:
            pass
        logger.info("JAR: %s exists=%s size=%s", j, exists, size)
        if not exists:
            missing.append(j)
    return missing


def create_spark_session(jars_arg, mongo_uri):
    builder = (
        SparkSession.builder
        .appName("MongoToIceberg")
        .config("spark.jars", jars_arg)


        .config("spark.sql.defaultCatalog", "nessie")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", NESSIE_REF)
        .config("spark.sql.catalog.nessie.warehouse", S3_WAREHOUSE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")


        .config("spark.mongodb.read.connection.uri", mongo_uri)
        .config("spark.mongodb.write.connection.uri", mongo_uri)


        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", S3_PATH_STYLE)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", S3_SSL)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
    )
    logger.info("Creating SparkSession...")
    spark = builder.getOrCreate()
    logger.info("SparkSession created. Spark version=%s", spark.version)
    return spark


def main():
    logger.info("START main at %s", nowts())
    missing = check_jars()
    if missing:
        logger.error("Missing JARs detected: %s", missing)
        raise SystemExit(
            "Required JARs missing - place them into ./jars and restart")

    jars_arg = ",".join(JARS)
    mongo_uri = build_mongo_uri()
    masked_mongo = mongo_uri.replace(
        MONGO_PASS, "****") if MONGO_PASS else mongo_uri
    logger.info("Mongo URI (masked): %s", masked_mongo)
    logger.info("Using Iceberg warehouse (must be s3a://): %s", S3_WAREHOUSE)
    logger.info("S3 endpoint=%s path_style=%s ssl_enabled=%s",
                S3_ENDPOINT, S3_PATH_STYLE, S3_SSL)
    logger.info("Nessie URI=%s ref=%s", NESSIE_URI, NESSIE_REF)

    try:
        spark = create_spark_session(jars_arg, mongo_uri)
        logger.info("spark.jars config=%s",
                    spark.conf.get("spark.jars", "<not-set>"))
        logger.info("spark.mongodb.read.connection.uri=%s",
                    spark.conf.get("spark.mongodb.read.connection.uri"))
        logger.info("spark.sql.catalog.nessie.warehouse=%s",
                    spark.conf.get("spark.sql.catalog.nessie.warehouse"))

        logger.info("Starting read from MongoDB collection %s.%s",
                    MONGO_DB, MONGO_COLL)
        df = (
            spark.read
            .format("mongodb")
            .option("uri", mongo_uri)
            .option("database", MONGO_DB)
            .option("collection", MONGO_COLL)
            .load()
        )

        if "_id" in df.columns:
            try:
                df = df.withColumn("_id", col("_id").cast("string"))
            except Exception as e:
                logger.warning("Could not cast _id to string: %s", str(e))

        logger.info("Read succeeded. Counting rows (this may be slow)...")
        try:
            count = int(df.count())
        except Exception as e:
            logger.warning(
                "Failed to count rows; assuming 0. Error: %s", str(e))
            count = 0
        logger.info("Rows read from MongoDB: %d", count)

        logger.info("Showing up to 5 rows:")
        try:
            rows = df.take(5)
            for r in rows:
                logger.info("ROW: %s", r)
        except Exception as e:
            logger.warning("Failed to take sample rows: %s", str(e))

        try:
            json_rows = df.limit(10).toJSON().collect()
            for jr in json_rows:
                logger.info(jr)
        except Exception as e:
            logger.error("Failed to print JSON rows: %s", str(e))

        namespace = MONGO_DB
        table_name = MONGO_COLL
        table_ident = "nessie." + namespace + "." + table_name

        namespace_location = S3_WAREHOUSE + \
            namespace if S3_WAREHOUSE.endswith(
                "/") else S3_WAREHOUSE + "/" + namespace
        parquet_target_path = (S3_WAREHOUSE.rstrip(
            "/") + "/" + namespace + "/" + table_name + "/parquet/")

        logger.info("Namespace: %s, Table: %s, Table ident: %s",
                    namespace, table_name, table_ident)
        logger.info("Namespace location: %s", namespace_location)
        logger.info("Parquet target path (for fallback): %s",
                    parquet_target_path)

        if count == 0 or (df is None) or (len(df.columns) == 0):
            logger.warning(
                "DataFrame appears empty or has no columns (rows=%d). Will write a job marker instead.", count)
            try:
                marker_path = S3_WAREHOUSE.rstrip(
                    "/") + "/" + namespace + "/" + table_name + "/_job_marker/"
                marker_df = spark.createDataFrame(
                    [("job_ran",)], "status STRING")
                marker_df.coalesce(1).write.mode("overwrite").option(
                    "header", "true").csv(marker_path)
                logger.info("Job marker written to %s", marker_path)
            except Exception as e:
                logger.warning("Failed to write job marker: %s", str(e))
        else:
            try:

                logger.info(
                    "Ensuring Iceberg namespace exists: nessie.%s", namespace)
                spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie." + namespace)
            except Exception as e:
                logger.warning(
                    "CREATE NAMESPACE failed (may already exist or need different permissions): %s", str(e))

            try:
                logger.info(
                    "Writing DataFrame as Iceberg table via Nessie catalog: %s", table_ident)
                df.writeTo(table_ident).createOrReplace()
                logger.info(
                    "Iceberg write successful for table: %s", table_ident)
            except Exception as e:
                logger.error("Iceberg write failed: %s", str(e))
                logger.info(
                    "Attempting fallback: write raw parquet then convert to Iceberg from parquet data")
                try:
                    logger.info(
                        "Writing Parquet to fallback path: %s", parquet_target_path)
                    df.write.mode("overwrite").option(
                        "compression", "snappy").parquet(parquet_target_path)
                    logger.info("Parquet write successful to %s",
                                parquet_target_path)
                    logger.info("Reading back parquet for conversion")
                    df_parquet = spark.read.parquet(parquet_target_path)
                    logger.info(
                        "Rows read from parquet for conversion: %d", df_parquet.count())
                    logger.info(
                        "Creating Iceberg table from parquet data: %s", table_ident)
                    df_parquet.writeTo(table_ident).createOrReplace()
                    logger.info(
                        "Converted parquet into Iceberg table: %s", table_ident)
                except Exception as e2:
                    logger.error(
                        "Fallback parquet->Iceberg conversion failed: %s", str(e2))
                    raise

        try:
            preview_limit = 100
            preview_df = df.limit(preview_limit)
            preview_path = S3_WAREHOUSE.rstrip(
                "/") + "/" + namespace + "/" + table_name + "/preview/"
            logger.info("Writing CSV preview (limit=%d) to %s",
                        preview_limit, preview_path)
            preview_df.coalesce(1).write.mode("overwrite").option(
                "header", "true").csv(preview_path)
            logger.info("CSV preview written to %s", preview_path)
        except Exception as e:
            logger.warning("Failed to write CSV preview: %s", str(e))

        try:
            logger.info(
                "Verifying Iceberg table read via Spark SQL: SELECT count(*) FROM %s", table_ident)
            cnt_row = spark.sql(
                "SELECT count(*) as cnt FROM " + table_ident).collect()
            logger.info("Verification count result: %s", cnt_row)
        except Exception as e:
            logger.warning(
                "Failed to verify Iceberg table via SQL: %s", str(e))

        logger.info("Stopping Spark.")
        spark.stop()
        logger.info("END main SUCCESS at %s", nowts())
        return "SUCCESS"

    except Exception as exc:
        logger.error("Exception during job: %s", str(exc))
        tb = traceback.format_exc()
        logger.error("Traceback:\n%s", tb)
        logger.error(
            "HINTS: - Ensure Mongo credentials are correct and authSource is right.")
        logger.error(
            " - Verify mongo container user/password (MONGO_INITDB_ROOT_USERNAME/PASSWORD).")
        logger.error(
            " - If using docker-compose, ensure 'mongo' and 'minio' hostnames resolve from Spark executors.")
        logger.error(
            " - Ensure ICEBERG_WAREHOUSE uses s3a:// and that hadoop-aws + aws-java-sdk + iceberg-spark-runtime jars are present.")
        raise


if __name__ == "__main__":
    main()
