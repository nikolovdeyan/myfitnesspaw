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
from typing import Union

import prefect

from . import _utils, flows


def run_etl_flow(
    user: str = None, **kwargs
) -> Union["prefect.engine.state.State", None]:
    """
    Create an ETL flow for the provided user and execute it locally.

    Args:
       - user (str): The name of the user to have the flow executed for
       - from_date (datetime.date, optional): The starting date (including) of the
         extraction sequence
       - to_date (datetime.date, optional): The ending date (incuding) of the extraction
         sequence
       - measures ([str], optional): A list of measures to be extracted

    Returns:
       - State: the state of the flow after the completed run.

    Raises:
       - ValueError: if the `user` keyword argument is not provided
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


def run_report_flow(
    user: str = None, parameters: dict = {}
) -> Union["prefect.engine.state.State", None]:
    """
    Create a report flow for the provided user and execute it locally.

    Args:
       - user (str): The name of the user to have the flow executed for
       - parameters (dict, optional): Parameters for the selected flow type

    Returns:
       - State: the state of the flow after the completed run.

    Raises:
       - ValueError: if the `user` keyword argument is not provided
    """
    flow = flows.get_progress_report_flow(user=user)

    flow.run_config = _utils.get_local_run_config()

    return flow.run(parameters=parameters)


def run_backup_flow() -> Union["prefect.engine.state.State", None]:
    """
    Create a backup flow and execute it locally.

    Returns:
       - State: the state of the flow after the completed run.
    """
    flow = flows.get_backup_flow()
    flow.run_config = _utils.get_local_run_config()

    return flow.run()


def register_etl_flow(
    user: str = None, project_name: str = None, flow_name: str = None
) -> Union["prefect.engine.state.State", None]:
    """
    Register a MyFitnessPaw ETL Flow to the Prefect Cloud for the provided user.

    Args:
       - user (str): The name of the user to have the flow registered for
       - project_name (str, optional): The name of the Prefect Cloud project that
         will be used to register the created flow. Note: The project must already
         exist in Prefect Cloud or the process will fail.
       - flow_name (str, optional): An optional name that will be used to register
         the created flow. If not provided a default name is applied.

    Returns:
       - State: the state of the flow after the completed run.

    Raises:
       - ValueError: if the `user` keyword argument is not provided
       - ValueError: if the `project_name` keyword argument is not provided
    """

    flow = flows.get_etl_flow(user=user, flow_name=flow_name)
    flow.run_config = _utils.get_local_run_config()
    return flow.register(project_name=project_name)


def register_report_flow(
    user: str = None,
    project_name: str = None,
    flow_name: str = None,
    report_type: str = None,
) -> Union["prefect.engine.state.State", None]:
    """
    Register a MyFitnessPaw report flow to the Prefect Cloud.

    Args:
       - user (str): The name of the user to have the flow registered for
       - project_name (str, optional): The name of the Prefect Cloud project that
         will be used to register the created flow. Note: The project must already
         exist in Prefect Cloud or the process will fail.
       - flow_name (str, optional): An optional name that will be used to register
         the created flow. If not provided a default name is applied.
       - report_type(str, optional): The type of report to register. Note: Currently
         only the weekly report is available and this parameter is not used.

    Returns:
       - State: the state of the flow after the completed run.

    Raises:
       - ValueError: if the `user` keyword argument is not provided
       - ValueError: if the `project_name` keyword argument is not provided
    """
    flow = flows.get_report_flow(
        user=user, report_type=report_type, flow_name=flow_name
    )
    flow.run_config = _utils.get_local_run_config()

    return flow.register(project_name=project_name)


def register_backup_flow(
    project_name: str = None, flow_name: str = None
) -> Union["prefect.engine.state.State", None]:
    """
    Register a MyFitnessPaw backup flow to the Prefect Cloud.

    Args:
       - project_name (str, optional): The name of the Prefect Cloud project that
         will be used to register the created flow. Note: The project must already
         exist in Prefect Cloud or the process will fail.
       - flow_name (str, optional): An optional name that will be used to register
         the created flow. If not provided a default name is applied.

    Returns:
       - State: the state of the flow after the completed run.

    Raises:
       - ValueError: if the `project_name` keyword argument is not provided
    """
    flow = flows.get_backup_flow(flow_name=flow_name)
    flow.run_config = _utils.get_local_run_config()
    return flow.register(project_name=project_name)
