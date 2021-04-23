"""
myfitnesspaw.api

This module defines the public API for the MyFitnessPaw project.

MyFitnessPaw consists of three separate instances of Prefect flows: the ETL flow is
responsible for the fetching, transforming, and loading of myfitnesspal data for the
respective user provided; the report flows compose and send email reports to the user;
and the backup flow is responsible for maintaining the project backups.

For each of the flows MyFitnessPaw exposes a number of functons for convenient usage:
  - the `run` function will immediately execute the flow in the current python process.
  - the `register` functions will register the flow against the Prefect Cloud account
    defined in the configuration.
"""
from . import _utils, flows


def register_all():
    raise NotImplementedError("API call not implemented.")


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
    return flow.run(parameters=parameters)


def run_report_flow(user=None, report_type=None):
    """
    Run a MyFitnessPaw Report Flow locally.
    """
    flow = flows.get_report_flow(user=user, report_type=report_type)
    flow.run_config = _utils.get_local_run_config()
    return flow.run()


def run_backup_flow():
    """
    Run a MyFitnessPaw Backup Flow locally.
    """
    flow = flows.get_backup_flow()
    flow.run_config = _utils.get_local_run_config()
    return flow.run()


def register_etl_flow(user=None, project_name=None, flow_name=None):
    """
    Register a MyFitnessPaw ETL Flow to the Prefect Cloud.
    """
    flow = flows.get_etl_flow(user=user, flow_name=flow_name)
    flow.run_config = _utils.get_local_run_config()
    return flow.register(project_name=project_name)


def register_report_flow(
    user=None, report_type=None, flow_name=None, project_name=None
):
    """
    Register a MyFitnessPaw Report Flow to the Prefect Cloud.
    """
    flow = flows.get_report_flow(
        user=user, report_type=report_type, flow_name=flow_name
    )
    flow.run_config = _utils.get_local_run_config()
    return flow.register(project_name=project_name)


def register_backup_flow(flow_name=None, project_name=None):
    """
    Schedule a MyFitnessPaw Backup Flow to the Prefect Cloud.
    """
    flow = flows.get_backup_flow(flow_name=flow_name)
    flow.run_config = _utils.get_local_run_config()
    return flow.register(project_name=project_name)
