"""

"""
import pendulum
import pathlib

from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from jinja2 import Environment, FileSystemLoader
import os


def build_sparkapplication_template(
    template_file_name: str,
    app_name: str,
    feature_id: str,
    image_tag: str,
    script_name: str,
) -> str:
    """
    Build sparkapplication manifest template for submitting to 'spark-on-k8s-operator'
    :param template_file_name: sparkapplication manifest template file name in 'templates' folder
    :param app_name: Spark Application name that will be a prefix of spark-submit
    :param feature_id: feature id that is unique metadata in AICNS project
    :return: Rendered template string for spark-submit to 'spark-on-k8s-operator'
    """
    templates_dir = os.path.join(
        os.path.realpath(os.path.dirname(__file__)), "../templates"
    )
    file_loader = FileSystemLoader(templates_dir)
    env = Environment(loader=file_loader)
    template = env.get_template(template_file_name)
    spark_app = {
        "name": app_name + "-{{ run_id.split('T')[0]|replace('__', '-') }}",
        "start": "{{ data_interval_start }}",
        "end": "{{ data_interval_end }}",
        "feature_id": feature_id,
        "image_tag": image_tag,
        "script_name": script_name,
    }
    sa = template.render(spark_app=spark_app)
    return sa


# todo: parameterize feature_id?
feature_id = "2"


# todo: templating for each features
# todo: start_date templating by using metadata
# todo: scaling concurrency, parallelism
with DAG(
    dag_id="data_pipeline_dag",
    start_date=pendulum.datetime(2022, 4, 30, tz="Asia/Seoul"),
    schedule_interval="@daily",
    max_active_tasks=4,
    max_active_runs=3,
    catchup=True,
) as dag:
    """
    Dag for managing one feature's data pipeline workflow.
    If contains this tasks
    - 1. aicns-data-validation-task (and sensor)
    - 2. aicns-regularity-analysis-task (and sensor)
    - 3. aicns-data-cleaning-task
    - and so on (will be updated)
    """

    validate_task_id = "validate_data_" + feature_id
    validate_data = SparkKubernetesOperator(
        task_id=validate_task_id,
        application_file=build_sparkapplication_template(
            template_file_name="sparkapplication-template.yaml",
            app_name=f"aicns-data-validation-task-{feature_id}",
            feature_id=feature_id,
            image_tag="youngminan/aicns-data-validation-task:latest",
            script_name="data_validator.py",
        ),
        namespace="default",
    )
    validate_data_sensor = SparkKubernetesSensor(
        task_id=validate_task_id + "_monitor",
        namespace="default",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{validate_task_id}')['metadata']['name'] }}}}",
    )
    
    analyze_regularity_task_id = "analyze_regularity_" + feature_id
    analyze_regularity = SparkKubernetesOperator(
        task_id=analyze_regularity_task_id,
        application_file=build_sparkapplication_template(
            template_file_name="sparkapplication-template.yaml",
            app_name=f"aicns-regularity-analysis-task-{feature_id}",
            feature_id=feature_id,
            image_tag="youngminan/aicns-regularity-analysis-task:latest",
            script_name="regularity_analyzer.py",
        ),
        namespace="default",
    )
    analyze_regularity_sensor = SparkKubernetesSensor(
        task_id=analyze_regularity_task_id + "_monitor",
        namespace="default",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{analyze_regularity_task_id}')['metadata']['name'] }}}}",
    )
    """
    clean_data_task_id = "clean_data_" + feature_id
    clean_data = SparkKubernetesOperator(
        task_id=clean_data_task_id,
        application_file=build_sparkapplication_template(
            template_file_name="sparkapplication-template.yaml",
            app_name=f"aicns-data-cleaning-task-{feature_id}",
            feature_id=feature_id,
            image_tag="youngminan/aicns-data-cleaning-task:latest",
            script_name="data_cleaner.py",
        ),
        namespace="default",
    )
    clean_data_sensor = SparkKubernetesSensor(
        task_id=clean_data_task_id + "_monitor",
        namespace="default",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{clean_data_task_id}')['metadata']['name'] }}}}",
    )
    """
    
    validate_data >> validate_data_sensor >> analyze_regularity >> analyze_regularity_sensor
