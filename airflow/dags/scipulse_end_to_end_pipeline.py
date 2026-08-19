from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator


# =============================================================================
# SciPulse - End-to-End Pipeline
# =============================================================================
#
# This DAG orchestrates the complete SciPulse workflow:
#
#   ArXiv Silver --------\
#   HN Silver ------------> Spark Gold -> Elasticsearch
#   Citations Silver ----/
#
# The three Silver pipelines run in parallel.
# The Gold Spark task starts only when all three have succeeded.
# =============================================================================


with DAG(
    dag_id="scipulse_end_to_end_pipeline",

    description=(
        "End-to-end SciPulse pipeline: "
        "Bronze -> Silver -> Spark Gold -> Elasticsearch"
    ),

    start_date=datetime(
        2026,
        1,
        1,
    ),

    schedule=None,

    catchup=False,

    tags=[
        "scipulse",
        "bronze",
        "silver",
        "gold",
        "spark",
        "elasticsearch",
    ],

) as dag:

    # =========================================================================
    # 1. ArXiv Silver pipeline
    # =========================================================================
    #
    # Existing DAG:
    #   arxiv_silver_pipeline
    #
    # It performs:
    #   Bronze validation
    #   -> quality profiling
    #   -> Great Expectations
    #   -> Silver cleaning
    #   -> upload to MinIO arxiv-clean/
    # =========================================================================

    run_arxiv_silver = TriggerDagRunOperator(
        task_id="run_arxiv_silver_pipeline",

        trigger_dag_id="arxiv_silver_pipeline",

        # Wait until the triggered DAG finishes.
        wait_for_completion=True,

        poke_interval=30,
    )


    # =========================================================================
    # 2. Hacker News Silver pipeline
    # =========================================================================
    #
    # Existing DAG:
    #   hn_silver_pipeline
    #
    # It performs:
    #   Bronze validation
    #   -> quality profiling
    #   -> Great Expectations
    #   -> Silver cleaning
    #   -> upload to MinIO hn-clean/
    # =========================================================================

    run_hn_silver = TriggerDagRunOperator(
        task_id="run_hn_silver_pipeline",

        trigger_dag_id="hn_silver_pipeline",

        wait_for_completion=True,

        poke_interval=30,
    )


    # =========================================================================
    # 3. Citations Silver pipeline
    # =========================================================================
    #
    # Existing DAG:
    #   citations_silver_pipeline
    #
    # It performs:
    #   Bronze citations check
    #   -> Great Expectations
    #   -> citations cleaning
    #   -> upload to MinIO citations-clean/
    # =========================================================================

    run_citations_silver = TriggerDagRunOperator(
        task_id="run_citations_silver_pipeline",

        trigger_dag_id="citations_silver_pipeline",

        wait_for_completion=True,

        poke_interval=30,
    )


    # =========================================================================
    # 4. Spark Gold task
    # =========================================================================
    #
    # Airflow uses Docker to execute spark-submit inside the existing
    # scipulse-spark container.
    #
    # The Spark Gold job:
    #
    #   - reads:
    #       arxiv-clean/
    #       hn-clean/
    #       citations-clean/
    #
    #   - builds the enriched impact dataset
    #
    #   - calculates:
    #       citation signal
    #       HN signal
    #       recency signal
    #       composite impact score
    #
    #   - writes the final Gold layer to:
    #
    #       Elasticsearch index:
    #       arxiv-papers-enriched
    #
    # elasticsearch-hadoop is loaded through --packages.
    # =========================================================================

    run_gold_spark = BashOperator(
        task_id="write_gold_to_elasticsearch",

        bash_command="""
        docker exec scipulse-spark \
          /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/ivy \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4 \
          /opt/spark/work-dir/src/gold/write_elasticsearch.py
        """,
    )


    # =========================================================================
    # Dependencies
    # =========================================================================
    #
    # The three Silver pipelines run in parallel.
    #
    # Gold is executed only if:
    #
    #   ArXiv Silver      = SUCCESS
    #   HN Silver         = SUCCESS
    #   Citations Silver  = SUCCESS
    #
    #                   ↓
    #
    #        Spark Gold -> Elasticsearch
    # =========================================================================

    [
        run_arxiv_silver,
        run_hn_silver,
        run_citations_silver,
    ] >> run_gold_spark