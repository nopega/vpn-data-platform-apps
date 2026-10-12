"""
End-to-end proof that a Spark job on this platform is wired together.

Not an ETL job. Each step fails loudly at the link that is broken, because
each of these links otherwise fails late or misleadingly:

  1. the image from Harbor has the Iceberg and S3 jars       (ClassNotFound)
  2. Spark reaches Polaris and authenticates as "etl"        (401 / connection refused)
  3. "etl" may create a namespace and a table                 (403 Forbidden)
  4. Polaris vends S3 credentials and the write lands         (AccessDenied naming the storage role)
  5. the rows read back are the rows written

Event logs (spark.eventLog.dir) prove the pod's own identity separately: the
driver will not start if Pod Identity or the spark-events/ marker is missing.

    kubectl apply -f runs/smoke-test.yaml
    kubectl -n spark get sparkapplication platform-smoke-test -w
    kubectl -n spark logs platform-smoke-test-driver
"""

import os
import sys

from pyspark.sql import SparkSession

CATALOG = "data_platform"
NAMESPACE = "bronze.smoke"
TABLE = f"{CATALOG}.{NAMESPACE}.platform_smoke_test"


def fail(msg: str) -> None:
    print(f"\nSMOKE TEST FAILED: {msg}\n", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    missing = [v for v in ("POLARIS_URI", "POLARIS_CREDENTIAL") if not os.environ.get(v)]
    if missing:
        fail("environment variables not set: " + ", ".join(missing))

    print("[1/5] SparkSession with the Polaris catalog")
    spark = (
        SparkSession.builder.appName("platform-smoke-test")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", os.environ["POLARIS_URI"])
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", CATALOG)
        .config(f"spark.sql.catalog.{CATALOG}.credential", os.environ["POLARIS_CREDENTIAL"])
        .config(f"spark.sql.catalog.{CATALOG}.scope", "PRINCIPAL_ROLE:ALL")
        # Ask Polaris for per-table S3 credentials. Spark's own role has no
        # access to the warehouse, so without these the write is denied.
        .config(f"spark.sql.catalog.{CATALOG}.header.X-Iceberg-Access-Delegation",
                "vended-credentials")
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.client.region", "ap-southeast-1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"      Spark {spark.version}")

    print("[2/5] Polaris: list namespaces")
    try:
        spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect()
    except Exception as exc:  # noqa: BLE001 -- the raw message is the point
        fail(f"cannot use Polaris at {os.environ['POLARIS_URI']}: {exc}")
    print("      ok")

    print(f"[3/5] create namespace {CATALOG}.{NAMESPACE}")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    except Exception as exc:  # noqa: BLE001
        fail(f"principal cannot create namespaces (catalog role grants?): {exc}")
    print("      ok")

    print(f"[4/5] write {TABLE}")
    rows = [(1, "alpha"), (2, "beta"), (3, "gamma")]
    df = spark.createDataFrame(rows, "id INT, label STRING")
    try:
        df.writeTo(TABLE).createOrReplace()
    except Exception as exc:  # noqa: BLE001
        fail(f"write failed (credential vending / storage role policy?): {exc}")
    print("      ok")

    print(f"[5/5] read {TABLE} back")
    got = sorted((r.id, r.label) for r in spark.table(TABLE).collect())
    if got != rows:
        fail(f"read back {got}, expected {rows}")
    location = spark.sql(f"DESCRIBE TABLE EXTENDED {TABLE}") \
        .filter("col_name = 'Location'").collect()
    print(f"      {len(got)} rows, location {location[0].data_type if location else '?'}")

    print("\nSMOKE TEST PASSED\n")
    spark.stop()


if __name__ == "__main__":
    main()
