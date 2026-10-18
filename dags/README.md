# dags/

Airflow's DAG folder: git-sync clones this repository and points Airflow at
this directory (`apps/airflow.yaml`, `dags.gitSync.subPath: dags`). A push to
`main` reaches the scheduler within about a minute; nothing is deployed.

| DAG | What it does |
|---|---|
| `platform_smoke_test` | proves git-sync, scheduler, KubernetesExecutor task pods, S3 task logs |
| `spark_smoke_test` | Airflow submits a SparkApplication and follows it to the end |
| `nyc_taxi_medallion` | the real pipeline: bronze → silver → gold, one day of NYC yellow taxi trips, daily at 10:00 Bangkok time |

New DAGs start paused. Trigger the smoke tests by hand; unpause
`nyc_taxi_medallion` only when you want the daily schedule.

## nyc_taxi_medallion

Ported from `nopega/airflow_dag`. The DAG file is unchanged except the
default `image_tag` (`0.2.0`, built and pushed by CI from tag `spark-v0.2.0`). The
three templates in `spark/` were changed only where the platform differs
(marked at the top of each file):

| | old platform | here |
|---|---|---|
| event logs | `s3a://data-store-prod-logs/spark-events/` | `s3a://vpn-data-platform-logs-126052242139/spark-events/` |
| S3 credentials | IRSA (`WebIdentityTokenFileCredentialsProvider`) | Pod Identity (`DefaultCredentialsProvider`) |
| Polaris | `polaris.data-platform.svc` | `polaris.polaris.svc` |
| warehouse | `data-store-prod-warehouse` | `vpn-data-platform-warehouse-126052242139` |
| table credentials | (vended, implicitly) | vended, requested explicitly with the `X-Iceberg-Access-Delegation` header -- Spark's own role has no warehouse access |

The PySpark code is NOT here. Each run's driver downloads it from
`github.com/nopega/ice-berg-data-pipeline` (`pipeline_repo` / `pipeline_ref`
params), unchanged.

Before the first run, the Polaris namespaces the jobs write into must exist:
`bronze.transactional`, `silver.derived`, `gold.aggregate`.
`bootstrap/spark.sh` (VPN_DATA_PLATFORM) creates them.

Failure email (`send_smtp_notification`) has no `smtp_default` connection
yet, so a failed run logs a notifier error instead of sending mail. BACKLOG:
"Airflow email alerts".

Trigger one day by hand: Trigger DAG w/ config → `{"date": "2024-01-15"}`.
