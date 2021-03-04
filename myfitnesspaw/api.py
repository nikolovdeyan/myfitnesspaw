"""
myfitnesspaw.api

This module defines the public API for the MyFitnessPaw project.
"""
from . import _utils, flows
from .schedules import BACKUP_DEFAULT, ETL_DEFAULT, WEEKLY_REPORT_DEFAULT


def register_etl_flow(
    user=None, project_name=None, flow_name=None, schedule=ETL_DEFAULT
):
    """
    Register a MyFitnessPaw ETL Flow to the Prefect Cloud.
    """
    flow = flows.get_etl_flow(user=user, flow_name=flow_name)
    flow.run_config = _utils.get_local_run_config()
    flow_id = flow.register(project_name=project_name)
    return flow_id


def register_report_flow(
    user=None,
    report_type=None,
    flow_name=None,
    project_name=None,
    schedule=WEEKLY_REPORT_DEFAULT,
):
    """
    Register a MyFitnessPaw Report Flow to the Prefect Cloud.
    """
    flow = flows.get_report_flow(
        user=user, report_type=report_type, flow_name=flow_name
    )
    flow.run_config = _utils.get_local_run_config()
    flow_id = flow.register(project_name=project_name)
    return flow_id


def register_backup_flow(flow_name=None, project_name=None, schedule=BACKUP_DEFAULT):
    """
    Register a MyFitnessPaw Backup Flow to the Prefect Cloud.
    """
    flow = flows.get_backup_flow(flow_name=flow_name)
    flow.run_config = _utils.get_local_run_config()
    flow_id = flow.register(project_name=project_name)
    return flow_id


def run_etl_flow(user=None, **kwargs):
    """
    Create an ETL Flow for user and execute it locally.
    """
    flow = flows.get_etl_flow(user=user)
    # prepare parameters to pass at runtime
    parameters = {}
    if kwargs.get("from_date"):
        parameters["from_date"] = kwargs.get("from_date")
    if kwargs.get("to_date"):
        parameters["to_date"] = kwargs.get("to_date")
    if kwargs.get("measures"):
        parameters["measures"] = kwargs.get("measures")
    flow.run_config = _utils.get_local_run_config()
    state = flow.run(parameters=parameters)
    return state


def run_report_flow(user=None, report_type=None):
    """
    Run a MyFitnessPaw Report Flow locally.
    """
    flow = flows.get_report_flow(user=user, report_type=report_type)
    flow.run_config = _utils.get_local_run_config()
    state = flow.run()
    return state


def run_backup_flow():
    """
    Run a MyFitnessPaw Backup Flow locally.
    """
    flow = flows.get_backup_flow()
    flow.run_config = _utils.get_local_run_config()
    state = flow.run()
    return state
