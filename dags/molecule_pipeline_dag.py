from datetime import timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param
from airflow.utils.trigger_rule import TriggerRule

from lib.molecule_pipeline.constants import (
    DEFAULT_MAX_MOLECULES,
    DEFAULT_N_CLUSTERS,
    DEFAULT_OVERWRITE,
)
from lib.molecule_pipeline.discovery import check_input_files, resolve_dataset
from lib.molecule_pipeline.generation import generate_molecules
from lib.molecule_pipeline.properties import calculate_properties
from lib.utils.teams import send_teams_alert


with DAG(
    dag_id='molecule_pipeline_dag',
    schedule='@weekly',
    start_date=None,
    catchup=False,
    tags=['molecules', 'cheminformatics', 'de_school'],
    params={
        'dataset_id': Param(default=None, type=['null', 'string']),
        'overwrite': Param(default=DEFAULT_OVERWRITE, type='boolean'),
        'max_molecules': Param(default=DEFAULT_MAX_MOLECULES, type='integer'),
        'n_clusters': Param(default=DEFAULT_N_CLUSTERS, type='integer'),
    },
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        'owner': 'data-platform',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30),
        'on_failure_callback': send_teams_alert,
    },
) as dag:
    start_op = EmptyOperator(task_id='start')

    resolve_dataset_op = PythonOperator(
        task_id='resolve_dataset',
        python_callable=resolve_dataset,
    )

    check_input_files_op = PythonOperator(
        task_id='check_input_files',
        python_callable=check_input_files,
    )

    generate_molecules_op = PythonOperator(
        task_id='generate_molecules',
        python_callable=generate_molecules,
    )

    generated_quality_checks_op = EmptyOperator(task_id='generated_quality_checks')

    calculate_properties_op = PythonOperator(
        task_id='calculate_properties',
        python_callable=calculate_properties,
    )

    properties_quality_checks_op = EmptyOperator(task_id='properties_quality_checks')
    cluster_molecules_op = EmptyOperator(task_id='cluster_molecules')
    clustered_quality_checks_op = EmptyOperator(task_id='clustered_quality_checks')

    finish_op = EmptyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    (
        start_op
        >> resolve_dataset_op
        >> check_input_files_op
        >> generate_molecules_op
        >> generated_quality_checks_op
        >> calculate_properties_op
        >> properties_quality_checks_op
        >> cluster_molecules_op
        >> clustered_quality_checks_op
        >> finish_op
    )
