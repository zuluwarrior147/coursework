import os
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

SCRIPTS_IMAGE = "ghcr.io/zuluwarrior147/coursework-scripts:latest"
SPARK_IMAGE = "ghcr.io/zuluwarrior147/coursework-spark:latest"
STORAGE_NAME = "coursework-storage"

volume = k8s.V1Volume(
    name=STORAGE_NAME,
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name=STORAGE_NAME
    ),
)
volume_mount = k8s.V1VolumeMount(name=STORAGE_NAME, mount_path="/tmp/", sub_path=None)

with DAG(
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule_interval=None,
    dag_id="coursework_dag",
) as dag:
    
    clean_storage_before_start = KubernetesPodOperator(
        name="clean_storage_before_start",
        image=SCRIPTS_IMAGE,
        cmds=["rm", "-rf", "/tmp/*"],
        task_id="clean_storage_before_start",
        is_delete_operator_pod=False,
        namespace="default",
        startup_timeout_seconds=600,
        image_pull_policy="Always",
        volumes=[volume],
        volume_mounts=[volume_mount],
    )

    load_data = KubernetesPodOperator(
        name="load_data",
        image=SCRIPTS_IMAGE,
        cmds=["python", "scripts/cli.py", "load", "--path", "/tmp/data"],
        task_id="load_data",
        in_cluster=False,
        is_delete_operator_pod=False,
        namespace="default",
        startup_timeout_seconds=600,
        image_pull_policy="Always",
        volumes=[volume],
        volume_mounts=[volume_mount],
    )

    run_spark_script = KubernetesPodOperator(
        name="run_spark_script",
        image=SPARK_IMAGE,
        cmds=["spark-submit", "/app/aggregate_datasets.py"],
        task_id="run_spark_script",
        in_cluster=False,
        is_delete_operator_pod=False,
        namespace="default",
        startup_timeout_seconds=600,
        image_pull_policy="Always",
        volumes=[volume],
        volume_mounts=[volume_mount],
    )

    # upload_data = KubernetesPodOperator(
    #     name="upload_data",
    #     image=DOCKER_IMAGE,
    #     cmds=[
    #         "python",
    #         "scripts/cli.py",
    #         "upload",
    #         "--bucket-name",
    #         "coursework-storage",
    #         "--data-path",
    #         "/tmp/data",
    #     ],
    #     task_id="upload_data",
    #     in_cluster=False,
    #     is_delete_operator_pod=False,
    #     namespace="default",
    #     startup_timeout_seconds=600,
    #     image_pull_policy="Always",
    #     volumes=[volume],
    #     volume_mounts=[volume_mount],
    # )

    clean_up = KubernetesPodOperator(
        name="clean_up",
        image=SCRIPTS_IMAGE,
        cmds=["rm", "-rf", "/tmp/*"],
        task_id="clean_up",
        in_cluster=False,
        is_delete_operator_pod=False,
        namespace="default",
        startup_timeout_seconds=600,
        image_pull_policy="Always",
        volumes=[volume],
        volume_mounts=[volume_mount],
        trigger_rule="all_done",
    )

    clean_storage_before_start >> load_data >> run_spark_script >> clean_up
