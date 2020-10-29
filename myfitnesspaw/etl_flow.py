"""MyFitnessPaw's Extract-Transform-Load Prefect Flow.

This Prefect flow contains the steps to extract information from www.myfitnesspal.com,
transform and store it locally in an SQLite database. The raw objects' data is stored in
a serialized JSON form which is then used to prepare several report-friendly tables.
"""
import datetime
import sqlite3
from contextlib import closing
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import jsonpickle
import myfitnesspal
import prefect
import requests
from myfitnesspal.exercise import Exercise
from myfitnesspal.meal import Meal
from prefect import Flow, Parameter, flatten, mapped, task, unmapped
from prefect.engine.state import State
from prefect.tasks.database.sqlite import SQLiteScript
from prefect.tasks.secrets import EnvVarSecret

from . import sql


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


create_mfp_database_script = f"""
{sql.create_raw_day_table}
{sql.create_notes_table}
{sql.create_water_table}
{sql.create_goals_table}
{sql.create_meals_table}
{sql.create_mealentries_table}
{sql.create_cardioexercises_table}
{sql.create_strengthexercises_table}
{sql.create_measurements_table}
"""
create_mfp_database = SQLiteScript(
    name="Create MyFitnessPaw DB (if not existing)",
    script=create_mfp_database_script,
)


def slack_notify_on_failure(flow: Flow, old_state: State, new_state: State) -> State:
    """State handler for Slack notifications in case of flow failure."""
    logger = prefect.context.get("logger")
    slack_hook_url = EnvVarSecret("MYFITNESSPAW_SLACK_WEBHOOK_URL")
    if new_state.is_failed():
        if not slack_hook_url.run():
            logger.info("No Slack hook url provided, skipping notification...")
            return new_state
        msg = f"MyFitnessPaw ETL flow has failed: {new_state}!"
        requests.post(slack_hook_url.run(), json={"text": msg})
    return new_state


@task(name="Prepare MFP Database Directory")
def create_mfp_db_directory(db):
    """Prepare the database directory."""
    db_dir = "/".join([d for d in db.split("/")[:-1]])
    root_dir = Path().cwd()
    db_path = root_dir.joinpath(Path(db_dir))
    db_path.mkdir(parents=True, exist_ok=True)


@task(name="Prepare Dates Sequence to Extract")
def generate_dates_to_extract(
    from_date_str: str, to_date_str: str
) -> List[datetime.date]:
    """Generate a list of dates between a start date and end date."""
    from_date = datetime.datetime.strptime(from_date_str, "%Y/%m/%d").date()
    to_date = datetime.datetime.strptime(to_date_str, "%Y/%m/%d").date()
    delta_days = (to_date - from_date).days
    #  including both the starting and ending date
    return [from_date + timedelta(days=i) for i in range(delta_days + 1)]


@task(
    name="Get Day Record for Date <- (myfitnesspal)",
    timeout=15,
    max_retries=10,
    retry_delay=timedelta(seconds=5),
)
def get_myfitnesspal_day(
    username: str, password: str, date: datetime.date
) -> MaterializedDay:
    """Get the myfitnesspal Day object for a provided date."""
    client = myfitnesspal.Client(username=username, password=password)
    myfitnesspal_day = client.get_date(date)
    #  Materialize lazy loading properties:
    day = MaterializedDay(
        username=username,
        date=date,
        # TODO: meals=[meal for meal in myfitnesspal_day.meals if meal]
        meals=myfitnesspal_day.meals,
        exercises=myfitnesspal_day.exercises,
        goals=myfitnesspal_day.goals,
        notes=myfitnesspal_day.notes.as_dict(),
        water=myfitnesspal_day.water,
    )
    return day


@task(
    name="Get Measure Records for Dates <- (myfitnesspal)",
    timeout=15,
    max_retries=10,
    retry_delay=timedelta(seconds=5),
)
def get_myfitnesspal_measure(
    measure: str,
    username: str,
    password: str,
    from_date_str: str,
    to_date_str: str,
) -> List[Tuple[str, datetime.date, str, float]]:
    """Get the myfitnesspal measure records for a provided measure and time range."""
    logger = prefect.context.get("logger")
    from_date = datetime.datetime.strptime(from_date_str, "%Y/%m/%d").date()
    to_date = datetime.datetime.strptime(to_date_str, "%Y/%m/%d").date()

    client = myfitnesspal.Client(username=username, password=password)
    try:
        records = client.get_measurements(measure, from_date, to_date)
    except ValueError:
        logger.warning(f"No measurement records found for '{measure}' measure!")
        return []
    return [(username, date, measure, value) for date, value in records.items()]


@task(name="Serialize Day Records List")
def serialize_myfitnesspal_days(
    myfitnesspal_days: Sequence,
) -> List[Tuple[str, datetime.date, str]]:
    """Prepare a list of serialized Day records."""
    days_values = [
        (day.username, day.date, jsonpickle.encode(day)) for day in myfitnesspal_days
    ]
    return days_values


@task(name="Filter New or Changed Day Records")
def filter_new_or_changed_records(
    extracted_records: Sequence[str], local_records: Sequence[str]
) -> List[str]:
    """Filter out extracted records that are locally available already."""
    logger = prefect.context.get("logger")
    records_to_upsert = [t for t in extracted_records if t not in local_records]
    logger.info(f"Records to Insert/Update: {len(records_to_upsert)}")
    return records_to_upsert


@task(name="Deserialize Day Records to Process")
def deserialize_records_to_process(
    serialized_days: Sequence[str],
) -> List[MaterializedDay]:
    """Deserialize a sequence of days."""
    result = []
    for day_json in serialized_days:
        day_decoded = jsonpickle.decode(day_json[2], classes=[MaterializedDay])
        result.append(day_decoded)
    return result


@task(name="Extract Notes from Day Sequence")
def extract_notes_from_days(
    days: Sequence[MaterializedDay],
) -> List[Tuple[str, datetime.date, Optional[Any], Optional[Any]]]:
    """Extract myfitnesspal Food Notes values from a sequence of myfitnesspal days."""
    result = [
        (
            day.username,
            day.date,
            day.notes.get("type"),
            day.notes.get("body"),
        )
        for day in days
        if day.notes["body"]
    ]
    return result


@task(name="Extract Water from Day Sequence")
def extract_water_from_days(days: Sequence[MaterializedDay]) -> List[Tuple]:
    """Extract myfitnesspal water values from a sequence of myfitnesspal days."""
    return [(day.username, day.date, day.water) for day in days]


@task(name="Extract Goals from Day Sequence")
def extract_goals_from_days(days: Sequence[MaterializedDay]) -> List[Tuple]:
    """Extract myfitnesspal daily goals from a sequence of myfitnesspal days."""
    return [
        (
            day.username,
            day.date,
            day.goals.get("calories", None),
            day.goals.get("carbohydrates", None),
            day.goals.get("fat", None),
            day.goals.get("protein", None),
            day.goals.get("sodium", None),
            day.goals.get("sugar", None),
        )
        for day in days
    ]


@task(name="Extract Meals from Day Sequence")
def extract_meals_from_days(days: Sequence[MaterializedDay]) -> List[Meal]:
    """Extract myfitnesspal Meal items from a sequence of myfitnesspal Days."""
    for day in days:
        for meal in day.meals:
            if not meal:  # TODO: ?
                continue
            else:
                meal.username = day.username
                meal.date = day.date
    return [meal for day in days for meal in day.meals if meal]


@task(name="Extract Meal Records from Meal Sequence")
def extract_meal_records_from_meals(meals: Sequence[Meal]) -> List[Tuple]:
    """Extract meal entry records from a sequence of myfitnesspal meals."""
    return [
        (
            meal.username,
            meal.date,
            meal.name,
            meal.totals.get("calories", None),
            meal.totals.get("carbohydrates", None),
            meal.totals.get("fat", None),
            meal.totals.get("protein", None),
            meal.totals.get("sodium", None),
            meal.totals.get("sugar", None),
        )
        for meal in meals
    ]


@task(name="Extract MealEntry Records from Meal Sequence")
def extract_mealentry_records_from_meals(meals: Sequence[Meal]) -> List[Tuple]:
    """Extract meal entries records from a sequence of myfitnesspal meals."""
    return [
        (
            meal.username,
            meal.date,
            meal.name,
            entry.short_name,
            entry.quantity,
            entry.unit,
            entry.totals.get("calories", None),
            entry.totals.get("carbohydrates", None),
            entry.totals.get("fat", None),
            entry.totals.get("protein", None),
            entry.totals.get("sodium", None),
            entry.totals.get("sugar", None),
        )
        for meal in meals
        for entry in meal.entries
    ]


@task(name="Extract Cardio Exercises from Day Sequence")
def extract_cardio_exercises_from_days(days: Sequence[MaterializedDay]) -> List[Tuple]:
    """Extract cardio exercise entries from a sequence of myfitnesspal days."""
    return [
        (
            day.username,
            day.date,
            record.name,
            record.nutrition_information.get("minutes", None),
            record.nutrition_information.get("calories burned", None),
        )
        for day in days
        for record in day.exercises[0]
    ]


@task(name="Extract Strength Exercises from Days Sequence")
def extract_strength_exercises_from_days(
    days: Sequence[MaterializedDay],
) -> List[Tuple]:
    """Extract strength exercise entries from a sequence of myfitnesspal days."""
    return [
        (
            day.username,
            day.date,
            record.name,
            record.nutrition_information.get("sets", None),
            record.nutrition_information.get("reps/set", None),
            record.nutrition_information.get("weight/set", None),
        )
        for day in days
        for record in day.exercises[1]
    ]


@task(name="Get Raw Day Records for Dates <- (MyFitnessPaw)")
def mfp_select_raw_days(
    username: str, dates: Sequence[datetime.date]
) -> List[Tuple[str, datetime.date, str]]:
    """Select raw day entries for username and provided dates."""
    db = prefect.context.parameters.get("sqlite_db_location", None)
    mfp_existing_days = []
    with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys = YES;")
        for date in dates:
            cursor.execute(sql.select_rawdaydata_record, (username, date))
            result = cursor.fetchone()
            day_json = result[0] if result else None
            day_record = (username, date, day_json)
            mfp_existing_days.append(day_record)
    return mfp_existing_days


@task(name="Load Raw Day Records -> (MyFitnessPaw)")
def mfp_insert_raw_days(days_values: Sequence[Tuple]) -> None:
    """Insert a sequence of day values in the database."""
    db = prefect.context.parameters.get("sqlite_db_location", None)
    with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys = YES;")
        cursor.executemany(sql.insert_or_replace_rawdaydata_record, days_values)
        conn.commit()


@task(name="Load Notes Records -> (MyFitnessPaw)")
def mfp_insert_notes(notes_values: Sequence[Tuple]) -> None:
    """Insert a sequence of note values in the database."""
    db = prefect.context.parameters.get("sqlite_db_location", None)
    with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys = YES;")
        cursor.executemany(sql.insert_note_record, notes_values)
        conn.commit()


@task(name="Load Water Records -> (MyFitnessPaw)")
def mfp_insert_water(water_values: Sequence[Tuple]) -> None:
    """Insert a sequence of water records values in the database."""
    db = prefect.context.parameters.get("sqlite_db_location", None)
    with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys = YES;")
        cursor.executemany(sql.insert_water_record, water_values)
        conn.commit()


@task(name="Load Goals Records -> (MyFitnessPaw)")
def mfp_insert_goals(goals_values: Sequence[Tuple]) -> None:
    """Insert a sequence of goals records in the database."""
    db = prefect.context.parameters.get("sqlite_db_location", None)
    with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys = YES;")
        cursor.executemany(sql.insert_goals_record, goals_values)
        conn.commit()


@task(name="Load Meal Records -> (MyFitnessPaw)")
def mfp_insert_meals(meals_values: Sequence[Tuple]) -> None:
    """Insert a sequence of meal values in the database."""
    db = prefect.context.parameters.get("sqlite_db_location", None)
    with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys = YES;")
        cursor.executemany(sql.insert_meal_record, meals_values)
        conn.commit()


@task(name="Load MealEntry Records -> (MyFitnessPaw)")
def mfp_insert_mealentries(mealentries_values: Sequence[Tuple]) -> None:
    """Insert a sequence of meal entry values in the database."""
    db = prefect.context.parameters.get("sqlite_db_location", None)
    with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys = YES;")
        cursor.executemany(sql.insert_mealentry_record, mealentries_values)
        conn.commit()


@task(name="Load CardioExercises Records -> (MyFitnessPaw)")
def mfp_insert_cardio_exercises(cardio_list: Sequence[Tuple]) -> None:
    """Insert a sequence of cardio exercise entries in the database."""
    db = prefect.context.parameters.get("sqlite_db_location", None)
    with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys = YES;")
        cursor.executemany(sql.insert_cardioexercises_command, cardio_list)
        conn.commit()


@task(name="Load StrengthExercises Records -> (MyFitnessPaw)")
def mfp_insert_strength_exercises(strength_list: Sequence[Tuple]) -> None:
    """Insert a sequence of strength exercise values in the database."""
    db = prefect.context.parameters.get("sqlite_db_location", None)
    with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys = YES;")
        cursor.executemany(sql.insert_strengthexercises_command, strength_list)
        conn.commit()


@task(name="Load Measurement Records -> (MyFitnessPaw)")
def mfp_insert_measurements(measurements: Sequence[Tuple]) -> None:
    """Insert a sequence of measurements in the database."""
    db = prefect.context.parameters.get("sqlite_db_location", None)
    with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys = YES;")
        cursor.executemany(sql.insert_measurements_command, measurements)
        conn.commit()


with Flow("MyFitnessPaw ETL Flow", state_handlers=[slack_notify_on_failure]) as flow:
    #  Gather required parameters/secrets
    username = EnvVarSecret("MYFITNESSPAW_USERNAME", raise_if_missing=True)
    password = EnvVarSecret("MYFITNESSPAW_PASSWORD", raise_if_missing=True)
    from_date = Parameter(
        name="from_date",
        required=False,
        default=(datetime.date.today() - timedelta(days=5)).strftime("%Y/%m/%d"),
    )
    to_date = Parameter(
        name="to_date",
        required=False,
        default=(datetime.date.today() - timedelta(days=1)).strftime("%Y/%m/%d"),
    )
    measures = Parameter(
        name="measures",
        required=False,
        default=["Weight"],
    )
    db_conn_str = Parameter(
        name="sqlite_db_location", required=False, default="database/mfp_db.sqlite"
    )
    #  Ensure database directory is available:
    database_dir_exists = create_mfp_db_directory(db=db_conn_str)

    #  Pass connection string to database creation task at runtime:
    database_exists = create_mfp_database(
        db=db_conn_str, upstream_tasks=[database_dir_exists]
    )

    #  Prepeare a sequence of dates to be extracted and get the day info for each:
    dates_to_extract = generate_dates_to_extract(from_date, to_date)
    extracted_days = get_myfitnesspal_day.map(
        date=dates_to_extract,
        username=unmapped(username),
        password=unmapped(password),
    )

    #  The days must be serialized in order to be stored in the database.
    serialized_extracted_days = serialize_myfitnesspal_days(extracted_days)

    #  Select the days that we already have in the database for the date range:
    mfp_existing_days = mfp_select_raw_days(
        username=username,
        dates=dates_to_extract,
        upstream_tasks=[database_exists],
    )

    #  Compare the existing records with the extracted records and filter out what
    #  is already available. Load only the new or changed raw days:
    serialized_days_to_process = filter_new_or_changed_records(
        extracted_records=serialized_extracted_days,
        local_records=mfp_existing_days,
    )
    raw_days_load_state = mfp_insert_raw_days(serialized_days_to_process)

    #  The sequence of filtered records to process will be deserialized before
    #  populating the reporting table to make extracting the information easier.
    days_to_process = deserialize_records_to_process(
        serialized_days=serialized_days_to_process, upstream_tasks=[raw_days_load_state]
    )

    #  Extract and load notes for each day:
    #  Currently only food notes are being processed.
    notes_records = extract_notes_from_days(days_to_process)
    notes_load_state = mfp_insert_notes(notes_records)

    #  Extract and load water intake records for each day:
    water_records = extract_water_from_days(days_to_process)
    water_load_state = mfp_insert_water(water_records)

    #  Extract and load daily goals:
    goals_records = extract_goals_from_days(days_to_process)
    goals_load_state = mfp_insert_goals(goals_records)

    #  Prepare a sequences of all meals and meal entries and load:
    meals_to_process = extract_meals_from_days(days_to_process)
    meals_records = extract_meal_records_from_meals(meals_to_process)
    mealentries_records = extract_mealentry_records_from_meals(meals_to_process)
    meals_load_state = mfp_insert_meals(meals_records)
    mealentries_load_state = mfp_insert_mealentries(
        mealentries_records, upstream_tasks=[meals_load_state]
    )

    #  Extract and load exercises:
    cardio_exercises_to_process = extract_cardio_exercises_from_days(days_to_process)
    strength_exercises_to_process = extract_strength_exercises_from_days(
        days_to_process
    )
    cardio_exercises_load_state = mfp_insert_cardio_exercises(
        cardio_list=cardio_exercises_to_process,
    )
    strength_exercises_load_state = mfp_insert_strength_exercises(
        strength_list=strength_exercises_to_process,
    )

    #  Measurements are extracted through a separate request to myfitnesspal unrelated
    #  to the Day object we are working with in the previous steps. There is a separate
    #  request made for each of the measures the user is tracking and the result is an
    #  OrderedDict with the measurements for the whole date range.
    measurements_records = get_myfitnesspal_measure(
        measure=mapped(measures),
        username=username,
        password=password,
        from_date_str=from_date,
        to_date_str=to_date,
    )
    measurements_load_state = mfp_insert_measurements(
        measurements=flatten(measurements_records),
        upstream_tasks=[database_exists],
    )

if __name__ == "__main__":
    flow_state = flow.run()
