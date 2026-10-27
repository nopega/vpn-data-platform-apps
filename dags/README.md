# dags/

Airflow's DAG folder: git-sync clones this repository and points Airflow at
this directory (`apps/airflow.yaml`, `dags.gitSync.subPath: dags`). A push to
`main` reaches the scheduler within about a minute; nothing is deployed.

One folder per pipeline. Each DAG's SparkApplication templates sit next to it
in `templates/`: Airflow resolves `application_file` relative to the DAG
file's own folder, so `templates/bronze.yaml` means *this pipeline's* bronze.

```
dags/
  taxi/          nyc_taxi_medallion.py + templates/{bronze_ingest,silver_clean,gold_aggregate}.yaml
  earthquakes/   usgs_earthquakes.py   + templates/{bronze,silver,gold}.yaml
  smoke_tests/   platform_smoke_test.py, spark_smoke_test.py + templates/smoke_test.yaml
```

A new pipeline is a new folder.

| DAG | What it does |
|---|---|
| `platform_smoke_test` | proves git-sync, scheduler, KubernetesExecutor task pods, S3 task logs |
| `spark_smoke_test` | Airflow submits a SparkApplication and follows it to the end |
| `nyc_taxi_medallion` | the real pipeline: bronze → silver → gold, one day of NYC yellow taxi trips, daily at 10:00 Bangkok time |
| `usgs_earthquakes` | incremental query-based CDC from the USGS API → SCD Type 2 (`event_history`) + SCD Type 1 (`event_current`) → daily summary; runs as Polaris principal `pl_earthquakes`, which can touch only the `*.earthquakes` namespaces. Daily at 10:30 Bangkok time |

New DAGs start paused. Trigger the smoke tests by hand; unpause
`nyc_taxi_medallion` only when you want the daily schedule.

## nyc_taxi_medallion

Ported from `nopega/airflow_dag`. The DAG file is unchanged except the
default `image_tag` (`0.2.0`, built and pushed by CI from tag `spark-v0.2.0`). The
three templates in `taxi/templates/` were changed only where the platform differs
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

Tables live in the domain namespaces `bronze.transactional.taxi`,
`silver.derived.taxi`, `gold.aggregate.taxi`, and every task runs as Polaris
principal `pl_taxi` (Secret `spark/polaris-pl-taxi`), which can touch only
those three. Before the first run:
`VPN_DATA_PLATFORM/bootstrap/pipeline_principal.sh taxi`.

Failure email (`send_smtp_notification`) has no `smtp_default` connection
yet, so a failed run logs a notifier error instead of sending mail. BACKLOG:
"Airflow email alerts".

Trigger one day by hand: Trigger DAG w/ config → `{"date": "2024-01-15"}`.
