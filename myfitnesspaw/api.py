"""
MyFitnessPaw API
"""
from . import _utils, flows
from .schedules import BACKUP_DEFAULT, ETL_DEFAULT, WEEKLY_REPORT_DEFAULT


def register_etl_flow(user=None, schedule=ETL_DEFAULT):
    """Register a MyFitnessPaw ETL Flow to the Prefect Cloud."""
    pass


def register_report_flow(user=None, type=None, schedule=WEEKLY_REPORT_DEFAULT):
    """Register a MyFitnessPaw Report Flow to the Prefect Cloud."""
    pass


def register_backup_flow(schedule=BACKUP_DEFAULT):
    """Register a MyFitnessPaw Backup Flow to the Prefect Cloud."""
    pass


def register_all_flows(schedules=[ETL_DEFAULT, BACKUP_DEFAULT, WEEKLY_REPORT_DEFAULT]):
    """Register all MyFitnessPaw Flows for all available users to the Prefect Cloud."""
    pass


def run_etl_flow(user=None, **kwargs):
    """Create an ETL Flow for user and execute it locally."""
    flow_name = f"MyFitnessPaw ETL <{user.upper()}>"
    flow = flows.get_etl_flow(user=user, flow_name=flow_name)

    # prepare parameters to pass at runtime
    parameters = {}
    if kwargs.get("from_date"):
        parameters["from_date"] = kwargs.get("from_date")
    if kwargs.get("to_date"):
        parameters["to_date"] = kwargs.get("to_date")
    if kwargs.get("measures"):
        parameters["measures"] = kwargs.get("measures")

    flow.run_config = _utils.get_local_run_config()
    flow.run(parameters=parameters)


def run_report_flow(user=None, type=None):
    """Run a MyFitnessPaw Report Flow locally."""
    pass


def run_backup_flow():
    """Run a MyFitnessPaw Backup Flow locally."""
    pass
