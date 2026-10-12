# dags/

Airflow's DAG folder: git-sync clones this repository and points Airflow at
this directory (`apps/airflow.yaml`, `dags.gitSync.subPath: dags`). A push to
`main` reaches the scheduler within about a minute; nothing is deployed.

| DAG | Proves |
|---|---|
| `platform_smoke_test` | git-sync, scheduler, KubernetesExecutor task pods, S3 task logs |
| `spark_smoke_test` | Airflow submits a SparkApplication and follows it to the end (needs the Spark image `datapipeline:0.1.0`) |

Both are unscheduled -- trigger them in the UI.

## The original pipelines

The NYC taxi medallion DAG (`nyc_taxi_medallion`) lives in the original
platform's repository and in `nopega/airflow_dag`. It is not here yet because
its SparkApplication templates name the old platform's buckets
(`data-store-prod-*`), Polaris address (`polaris.data-platform.svc`),
IRSA credentials provider and Polaris namespaces created by scripts that do
not exist here. Bring it over by copying `spark/smoke_test.yaml`'s
conventions: the logs bucket and `DefaultCredentialsProvider` in
`sparkConf`, `POLARIS_URI=http://polaris.polaris.svc.cluster.local:8181/api/catalog`,
and the `spark-polaris-credentials` Secret.
