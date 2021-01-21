import os
import datetime
from datetime import timedelta

import prefect
from prefect import Flow, Parameter, task
from prefect.storage.local import Local
from prefect.run_configs import LocalRun


@task
def log_config_information():
    logger = prefect.context.get("logger")
    msg = f"""
    Running in: {os.getcwd()}
    ----
    $PATH: {os.environ.get('PATH', 'N/A')}
    $PYTHONPATH: {os.environ.get('PYTHONPATH', 'N/A')}
    $PREFECT__CLOUD__AGENT__LABELS: {os.environ.get('PREFECT__CLOUD__AGENT__LABELS', 'N/A')}
    -----
    Prefect system information: {prefect.utilities.diagnostics.system_information()}
    -----
    Prefect environment: {prefect.utilities.diagnostics.environment_variables()}
    -----
    Prefect config: {prefect.context.config}
    """
    logger.info(msg)


@task
def log_flow_information(f):
    logger = prefect.context.get("logger")
    msg = f"""
    Prefect flow information: {prefect.utilities.diagnostics.flow_information(f)}
    """
    logger.info(msg)


with Flow("Diagnostic Log Flow") as flow:
    # working_dir = ""
    # flow.run_config = LocalRun(
    #             working_dir=f"{working_dir}",
    #             env={"PYTHONPATH": ""},
    #         )
    flow.run_config = LocalRun()
    a = log_config_information()
    b = log_flow_information(flow)
