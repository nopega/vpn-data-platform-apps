"""Spark smoke test, driven by Airflow.

The same job as runs/smoke-test.yaml, submitted the way real pipelines will
be: an Airflow task pod (ServiceAccount airflow-worker) creates a
SparkApplication in namespace spark -- allowed by the airflow-spark-submitter
Role in workloads/spark -- waits for it, and copies the driver's output into
the Airflow task log, which then lands in S3.

Passing means the whole chain works:
    Airflow -> Kubernetes API -> Spark operator -> Harbor image
            -> Polaris (principal etl) -> vended S3 credentials -> warehouse

Not scheduled -- trigger it by hand.
"""

from __future__ import annotations

from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

with DAG(
    dag_id="spark_smoke_test",
    description="Airflow -> Spark operator -> Polaris -> S3, end to end.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["platform", "smoke-test", "spark"],
) as dag:
    SparkKubernetesOperator(
        task_id="spark_smoke_test",
        namespace="spark",
        application_file="templates/smoke_test.yaml",
        # Stream the driver's [1/5]... lines into this task's log.
        get_logs=True,
        # On failure, show the pod's Kubernetes events (ImagePullBackOff,
        # unschedulable) instead of a bare non-zero exit.
        log_events_on_failure=True,
        # Keep a failed driver so its log can still be read.
        delete_on_termination=False,
        do_xcom_push=False,
    )
