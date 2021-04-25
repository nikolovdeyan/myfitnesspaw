"""
myfitnesspaw._utils

MyFitnessPaw utilities Module.

Contains helper functions for various tasks.
"""

import datetime
import json
from dataclasses import dataclass
from typing import Dict, List, Optional

import myfitnesspal
import prefect
import requests
from myfitnesspal.exercise import Exercise
from myfitnesspal.meal import Meal
from prefect.core import Flow, Task
from prefect.engine.state import State
from prefect.run_configs import LocalRun
from prefect.tasks.secrets import PrefectSecret

from templates import slack_notifications

from . import MFP_CONFIG_PATH, PYTHONPATH, ROOT_DIR


def custom_terminal_state_handler(
    flow: Flow,
    state: State,
    task_states: Dict[Task, State],
) -> Optional[State]:
    """
    See: https://docs..prefect.io/core/concepts/flows.html#terminal-state-handlers
    """
    # iterate through task states, making a list of failing refernce tasks
    failed_tasks = []
    for task, task_state in task_states.items():
        if task_state.is_failed() and task in flow.reference_tasks():
            failed_tasks.append(task.name)
    # update the terminal state of the Flow and return
    state.message = "The following tasks failed: {}".format(failed_tasks)
    return state


def slack_fail_notification(flow: Flow, old_state: State, new_state: State) -> State:
    """
    State handler for Slack notifications in case of flow failure.

    Args:
       - flow (Flow): The flow to be observed for failure
       - old_state (State):
       - new_state (State):

    Returns:
       - State: The new flow state to be returned after notification is sent
    """

    logger = prefect.context.get("logger")
    slack_hook_url = PrefectSecret("MYFITNESSPAW_SLACK_WEBHOOK_URL")
    if new_state.is_failed():
        if not slack_hook_url.run():
            logger.warning("No Slack hook url provided, skipping notification...")
            return new_state

        msg = slack_notifications.FLOW_ERROR % (
            "MyFitnessPaw",
            flow.name,
            new_state.message,
        )
        slack_payload = json.loads(msg)
        requests.post(
            slack_hook_url.run(),
            json=slack_payload,
            headers={"Content-Type": "application/json"},
        )

    return new_state


def try_parse_date_str(date_str: str) -> datetime.datetime:
    """
    Try to parse a date string using a set of provided formats.

    Args:
       - date_str (str): A string to be parsed as a date using the available formats

    Returns:
       - datetime.datetime(): The parsed date

    Raises:
       - ValueError: If the provided string can't be parsed using the available formats
    """

    available_formats = ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%Y")

    for fmt in available_formats:
        try:
            return datetime.datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    raise ValueError(f"No available format found to parse <{date_str}>.")


def select_fifo_backups_to_delete(max_num_backups: int, files_list: List) -> List[str]:
    """
    Return the oldest backups from a files_list to satisfy the max_num_backups.

    Args:
       - max_num_backups (int): The maximum number of backup files to keep on the server
       - files_list (List): The list of available backup files on the server

    Returns:
       - List: The list with the oldest files on the server due to be deleted
    """

    timestamps = [
        datetime.datetime.strptime(f.split("_")[3], "%Y-%m-%d") for f in files_list
    ]
    timestamps.sort()
    if len(timestamps) <= max_num_backups:
        return []  # nothing to delete
    cut_index = len(timestamps) - max_num_backups
    return [f"mfp_db_backup_{ts.strftime('%Y-%m-%d')}" for ts in timestamps[:cut_index]]


def get_local_run_config() -> LocalRun:
    """
    Return a LocalRun configuration to attach to a flow.

    Returns:
       - prefect.run_configs.LocalRun: The local run configuration to be applied to a flow
    """
    return LocalRun(
        working_dir=ROOT_DIR,
        env={
            "PREFECT__USER_CONFIG_PATH": MFP_CONFIG_PATH,
            "PYTHONPATH": PYTHONPATH,
        },
    )


@dataclass
class MaterializedDay:
    """
    A class to hold the properties from myfitnesspal that we are working with.
    """

    username: str
    date: datetime.date
    meals: List[Meal]
    exercises: List[Exercise]
    goals: Dict[str, float]
    notes: Dict  # currently python-myfitnesspal only scrapes food notes
    water: float
    measurements: Dict[str, float]


class MyfitnesspalClientAdapter:
    """
    An adapter class to handle the external myfitnesspal dependency.
    """

    def __init__(self, username=None, password=None):
        if username is None or password is None:
            raise ValueError("Username and password arguments must be provided.")
        self._username = username
        self._password = password
        self._client = myfitnesspal.Client(self._username, self._password)

    def __enter__(self):
        return self

    def __exit__(self, err_type, err_value, err_traceback):
        self.close()

    def _get_measurements(self, date, measures):
        measurements = {}
        for measure in measures:
            try:
                response = self._client.get_measurements(measure, date, date)
                measurement_value = response.get(date, None)
                if measurement_value:
                    measurements[measure] = measurement_value
            except ValueError:
                print(f"No measure records found for {measure} measure.")
        return measurements

    def _get_date(self, date):
        return self._client.get_date(date)

    def get_myfitnesspaw_day(self, date, measures):
        day = self._get_date(date)
        measurements = self._get_measurements(date, measures)
        return MaterializedDay(
            username=self._username,
            date=date,
            meals=day.meals,
            exercises=day.exercises,
            goals=day.goals,
            notes=day.notes.as_dict(),
            water=day.water,
            measurements=measurements,
        )

    def close(self):
        self._client = None
