"""
MyFitnessPaw's Extract-Transform-Load Prefect Flow.

This Prefect flow contains the steps to extract information from www.myfitnesspal.com,
transform and store it locally in an SQLite database. The raw objects' data is stored in
a serialized JSON form which is then used to prepare several report-friendly tables.
"""
import datetime
import sqlite3
from contextlib import closing
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import jsonpickle
import myfitnesspal
import prefect
import requests
from myfitnesspal.exercise import Exercise
from myfitnesspal.meal import Meal
from prefect import Flow, Parameter, flatten, mapped, task, unmapped
from prefect.engine.state import State
from prefect.tasks.secrets import PrefectSecret

import myfitnesspaw as mfp


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


@task(name="Parse From and To Dates Parameters")
def parse_date_parameters(
    from_date_str: Union[str, None], to_date_str: Union[str, None]
) -> Tuple[datetime.date, datetime.date]:
    """Returns parsed datetime.date objects from the parameters passed."""
    today = datetime.date.today()
    default_from_date = today - timedelta(days=6)
    default_to_date = today - timedelta(days=1)

    if from_date_str is None:
        from_date = default_from_date
    else:
        from_date = try_parse_date_str(from_date_str).date()
    if to_date_str is None:
        to_date = default_to_date
    else:
        to_date = try_parse_date_str(to_date_str).date()

    return (from_date, to_date)


@task(name="Prepare Dates Sequence to Extract")
def generate_dates_to_extract(
    from_date: datetime.date, to_date: datetime.date
) -> List[datetime.date]:
    """Generate a list of dates between a start date and end date."""
    delta_days = (to_date - from_date).days
    #  including both the starting and ending date
    return [from_date + timedelta(days=i) for i in range(delta_days + 1)]


@task(name="Prepare MFP Database")
def create_mfp_database():
    """Prepare the database directory and file."""
    create_mfp_db_script = f"""
    {mfp.sql.create_raw_day_table}
    {mfp.sql.create_notes_table}
    {mfp.sql.create_water_table}
    {mfp.sql.create_goals_table}
    {mfp.sql.create_meals_table}
    {mfp.sql.create_mealentries_table}
    {mfp.sql.create_cardioexercises_table}
    {mfp.sql.create_strengthexercises_table}
    {mfp.sql.create_measurements_table}
    """
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.executescript(create_mfp_db_script)
        conn.commit()


@task(
    name="Get Day Record for Date <- (myfitnesspal)",
    timeout=15,
    max_retries=5,
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
    max_retries=5,
    retry_delay=timedelta(seconds=5),
)
def get_myfitnesspal_measure(
    measure: str,
    username: str,
    password: str,
    dates_to_extract: List[datetime.date],
) -> List[Tuple[str, datetime.date, str, float]]:
    """Get the myfitnesspal measure records for a provided measure and time range."""
    logger = prefect.context.get("logger")
    from_date, to_date = min(dates_to_extract), max(dates_to_extract)
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
    mfp_existing_days = []
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        for date in dates:
            c.execute(mfp.sql.select_rawdaydata_record, (username, date))
            result = c.fetchone()
            day_json = result[0] if result else None
            day_record = (username, date, day_json)
            mfp_existing_days.append(day_record)
    return mfp_existing_days


@task(name="Load Raw Day Records -> (MyFitnessPaw)")
def mfp_insert_raw_days(days_values: Sequence[Tuple]) -> None:
    """Insert a sequence of day values in the database."""
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(mfp.sql.insert_or_replace_rawdaydata_record, days_values)
        conn.commit()


@task(name="Load Notes Records -> (MyFitnessPaw)")
def mfp_insert_notes(notes_values: Sequence[Tuple]) -> None:
    """Insert a sequence of note values in the database."""
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(mfp.sql.insert_note_record, notes_values)
        conn.commit()


@task(name="Load Water Records -> (MyFitnessPaw)")
def mfp_insert_water(water_values: Sequence[Tuple]) -> None:
    """Insert a sequence of water records values in the database."""
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(mfp.sql.insert_water_record, water_values)
        conn.commit()


@task(name="Load Goals Records -> (MyFitnessPaw)")
def mfp_insert_goals(goals_values: Sequence[Tuple]) -> None:
    """Insert a sequence of goals records in the database."""
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(mfp.sql.insert_goals_record, goals_values)
        conn.commit()


@task(name="Load Meal Records -> (MyFitnessPaw)")
def mfp_insert_meals(meals_values: Sequence[Tuple]) -> None:
    """Insert a sequence of meal values in the database."""
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(mfp.sql.insert_meal_record, meals_values)
        conn.commit()


@task(name="Load MealEntry Records -> (MyFitnessPaw)")
def mfp_insert_mealentries(mealentries_values: Sequence[Tuple]) -> None:
    """Insert a sequence of meal entry values in the database."""
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(mfp.sql.insert_mealentry_record, mealentries_values)
        conn.commit()


@task(name="Load CardioExercises Records -> (MyFitnessPaw)")
def mfp_insert_cardio_exercises(cardio_list: Sequence[Tuple]) -> None:
    """Insert a sequence of cardio exercise entries in the database."""
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(mfp.sql.insert_cardioexercises_command, cardio_list)
        conn.commit()


@task(name="Load StrengthExercises Records -> (MyFitnessPaw)")
def mfp_insert_strength_exercises(strength_list: Sequence[Tuple]) -> None:
    """Insert a sequence of strength exercise values in the database."""
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(mfp.sql.insert_strengthexercises_command, strength_list)
        conn.commit()


@task(name="Load Measurement Records -> (MyFitnessPaw)")
def mfp_insert_measurements(measurements: Sequence[Tuple]) -> None:
    """Insert a sequence of measurements in the database."""
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(mfp.sql.insert_measurements_command, measurements)
        conn.commit()


def get_flow_for_user(user) -> Flow:
    """Return a flow that gets the user data from myfitnesspal into the local database."""
    with Flow(
        f"MyFitnessPaw ETL <{user.upper()}>", state_handlers=[slack_notify_on_failure]
    ) as flow:
        flow.run_config = mfp.get_local_run_config()
        from_date, to_date = parse_date_parameters(
            from_date_str=Parameter(name="from_date", default=None),
            to_date_str=Parameter(name="to_date", default=None),
        )
        measures = Parameter(name="measures", default=["Weight"])
        username = PrefectSecret(f"MYFITNESSPAL_USERNAME_{user.upper()}")
        password = PrefectSecret(f"MYFITNESSPAL_PASSWORD_{user.upper()}")
        db_exists = create_mfp_database()
        dates_to_extract = generate_dates_to_extract(from_date, to_date)
        extracted_days = get_myfitnesspal_day.map(
            date=dates_to_extract,
            username=unmapped(username),
            password=unmapped(password),
        )
        serialized_extracted_days = serialize_myfitnesspal_days(extracted_days)
        mfp_existing_days = mfp_select_raw_days(
            username=username,
            dates=dates_to_extract,
            upstream_tasks=[db_exists],
        )
        serialized_days_to_process = filter_new_or_changed_records(
            extracted_records=serialized_extracted_days,
            local_records=mfp_existing_days,
        )
        raw_days_load_state = mfp_insert_raw_days(serialized_days_to_process)
        days_to_process = deserialize_records_to_process(
            serialized_days=serialized_days_to_process,
            upstream_tasks=[raw_days_load_state],
        )
        notes_records = extract_notes_from_days(days_to_process)
        notes_load_state = mfp_insert_notes(notes_records)  # NOQA
        water_records = extract_water_from_days(days_to_process)
        water_load_state = mfp_insert_water(water_records)  # NOQA
        goals_records = extract_goals_from_days(days_to_process)
        goals_load_state = mfp_insert_goals(goals_records)  # NOQA
        meals_to_process = extract_meals_from_days(days_to_process)
        meals_records = extract_meal_records_from_meals(meals_to_process)
        mealentries_records = extract_mealentry_records_from_meals(meals_to_process)
        meals_load_state = mfp_insert_meals(meals_records)
        mealentries_load_state = mfp_insert_mealentries(  # NOQA
            mealentries_records, upstream_tasks=[meals_load_state]
        )
        cardio_exercises_to_process = extract_cardio_exercises_from_days(
            days_to_process
        )
        strength_exercises_to_process = extract_strength_exercises_from_days(
            days_to_process
        )
        cardio_exercises_load_state = mfp_insert_cardio_exercises(  # NOQA
            cardio_list=cardio_exercises_to_process,
        )
        strength_exercises_load_state = mfp_insert_strength_exercises(  # NOQA
            strength_list=strength_exercises_to_process,
        )
        measurements_records = get_myfitnesspal_measure(
            measure=mapped(measures),
            username=username,
            password=password,
            dates_to_extract=dates_to_extract,
        )
        measurements_load_state = mfp_insert_measurements(  # NOQA
            measurements=flatten(measurements_records),
            upstream_tasks=[db_exists],
        )
    return flow
