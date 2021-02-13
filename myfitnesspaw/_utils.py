"""
MyFitnessPaw Utilities Module.

Contains helper functions for various tasks.
"""

import datetime
from typing import Dict, List, Sequence

import prefect
import requests
from myfitnesspal.exercise import Exercise
from myfitnesspal.meal import Meal
from prefect.core import Flow
from prefect.engine.state import State
from prefect.run_configs import LocalRun
from prefect.tasks.secrets import PrefectSecret

from . import MFP_CONFIG_PATH, PYTHONPATH, ROOT_DIR


def slack_notify_on_failure(flow: Flow, old_state: State, new_state: State) -> State:
    """State handler for Slack notifications in case of flow failure."""
    logger = prefect.context.get("logger")
    slack_hook_url = PrefectSecret("MYFITNESSPAW_SLACK_WEBHOOK_URL")
    if new_state.is_failed():
        if not slack_hook_url.run():
            logger.info("No Slack hook url provided, skipping notification...")
            return new_state
        msg = f"MyFitnessPaw ETL flow has failed: {new_state}!"
        requests.post(slack_hook_url.run(), json={"text": msg})
    return new_state


def try_parse_date_str(date_str: str) -> datetime.datetime:
    """Try to parse a date string using a set of provided formats."""
    available_formats = ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%Y")
    for fmt in available_formats:
        try:
            return datetime.datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    raise ValueError(f"No available format found to parse <{date_str}>.")


def select_fifo_backups_to_delete(
    max_num_backups: int, files_list: Sequence
) -> List[str]:
    """Return the oldest backups from a files_list to satisfy the max_num_backups."""
    timestamps = [
        datetime.datetime.strptime(f.split("_")[3], "%Y-%m-%d") for f in files_list
    ]
    timestamps.sort()
    if len(timestamps) <= max_num_backups:
        return []  # nothing to delete
    cut_index = len(timestamps) - max_num_backups
    return [f"mfp_db_backup_{ts.strftime('%Y-%m-%d')}" for ts in timestamps[:cut_index]]


def get_local_run_config() -> LocalRun:
    """Return a LocalRun configuration to attach to a flow."""
    return LocalRun(
        working_dir=ROOT_DIR,
        env={
            "PREFECT__USER_CONFIG_PATH": MFP_CONFIG_PATH,
            "PYTHONPATH": PYTHONPATH,
        },
    )


class MaterializedDay:
    """A class to hold the properties from myfitnesspal that we are working with."""

    def __init__(
        self,
        username: str,
        date: datetime.date,
        meals: List[Meal],
        exercises: List[Exercise],
        goals: Dict[str, float],
        notes: Dict,  # currently python-myfitnesspal only scrapes food notes
        water: float,
    ):
        self.username = username
        self.date = date
        self.meals = meals
        self.exercises = exercises
        self.goals = goals
        self.notes = notes
        self.water = water
