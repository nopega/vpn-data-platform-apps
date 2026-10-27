"""
USGS earthquakes -> bronze (CDC changelog) -> silver (SCD2 + SCD1) -> gold.

Incremental and watermark-driven: each run pulls only what USGS changed since
the newest version already landed, so a run is identified by WHEN it ran, not
by a processing date. Missed a day? The next run's watermark covers it.

WHAT THIS PIPELINE DEMONSTRATES
-------------------------------
- incremental load        bronze reads from its own watermark (max `updated`)
- CDC (query-based)       every distinct `updated` of an event is a version
- SCD Type 2              silver.derived.earthquakes.event_history
- SCD Type 1              silver.derived.earthquakes.event_current
- partitioning            bronze by days(_ingested_at), silver by days(event_time),
                          gold by event_date (Iceberg hidden partitioning)
- least privilege         every task runs as Polaris principal pl_earthquakes,
                          which can touch only the three earthquakes namespaces;
                          bronze starts by PROVING it cannot read the taxi table
- recovery                watermark only advances on a committed write; MERGEs
                          are keyed, so any task can be re-run; `since` re-reads
                          from any point in time

Source: https://earthquake.usgs.gov/fdsnws/event/1/ -- public, no key.
Code:   nopega/ice-berg-data-pipeline, pipelines/earthquakes/
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

LOCAL_TZ = pendulum.timezone("Asia/Bangkok")

with DAG(
    dag_id="usgs_earthquakes",
    description="USGS earthquakes: incremental CDC -> SCD2/SCD1 -> daily summary, in Iceberg",
    # 10:30 Bangkok, after the taxi pipeline's 10:00, so the two do not compete
    # for the same Spot capacity.
    schedule="30 10 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=LOCAL_TZ),
    # No catchup: the watermark already makes every run cover everything since
    # the last successful one. Catching up would only queue redundant runs.
    catchup=False,
    max_active_runs=1,
    tags=["usgs", "iceberg", "cdc", "scd", "daily"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    params={
        # Nullable so the trigger form can leave it blank.
        "since": Param(
            None,
            type=["null", "string"],
            pattern=r"^$|^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?)?$",
            description="blank = from the watermark; or YYYY-MM-DD[THH:MM] (UTC) to re-read from that point",
        ),
        "image_tag": Param("0.2.0", type="string", pattern=r"^[A-Za-z0-9._-]+$",
                           description="a tag in harbor.nopega.net/ice-berg-platform/datapipeline"),
        "pipeline_repo": Param("nopega/ice-berg-data-pipeline", type="string",
                               pattern=r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$",
                               description="owner/repo on GitHub, and it must be public"),
        "pipeline_ref": Param("refs/heads/main", type="string", pattern=r"^[A-Za-z0-9._/-]+$",
                              description="refs/heads/main, or a commit SHA to pin the run"),
    },
    doc_md=__doc__,
) as dag:
    common = dict(
        namespace="spark",
        get_logs=True,
        log_events_on_failure=True,
        delete_on_termination=False,
        do_xcom_push=False,
    )

    bronze = SparkKubernetesOperator(
        task_id="bronze_cdc",
        application_file="templates/bronze.yaml",
        doc_md="Governance check, then pull every event USGS changed since the watermark into the changelog.",
        **common,
    )
    silver = SparkKubernetesOperator(
        task_id="silver_scd",
        application_file="templates/silver.yaml",
        doc_md="SCD Type 2 history (event_history) and SCD Type 1 latest state (event_current).",
        **common,
    )
    gold = SparkKubernetesOperator(
        task_id="gold_daily",
        application_file="templates/gold.yaml",
        doc_md="Per event day and region: counts, magnitudes, and how much USGS revised them.",
        **common,
    )

    bronze >> silver >> gold
