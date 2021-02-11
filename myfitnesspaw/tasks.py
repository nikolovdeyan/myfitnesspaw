import datetime
import sqlite3
from contextlib import closing
from datetime import timedelta
from typing import Any, List, Optional, Sequence, Tuple, Union

import dropbox
import jinja2
import jsonpickle
import myfitnesspal
import prefect
from dropbox.files import WriteMode
from myfitnesspal.meal import Meal
from prefect import task
from prefect.tasks.notifications.email_task import EmailTask

from . import DB_PATH, TEMPLATES_DIR, sql
from ._utils import MaterializedDay, select_fifo_backups_to_delete, try_parse_date_str


@task(name="Parse From and To Dates Parameters")
def parse_date_parameters(
    from_date_str: Union[str, None], to_date_str: Union[str, None]
) -> Tuple[datetime.date, datetime.date]:
    """Returns parsed datetime.date objects from the parameters passed."""
    today = datetime.date.today()
    default_from_date = today - timedelta(days=6)
    default_to_date = today - timedelta(days=1)

    if (from_date_str is not None and to_date_str is None) or (
        from_date_str is None and to_date_str is not None
    ):
        raise ValueError(
            "Either both from_date and to_date should be provided or neither."
        )

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
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
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
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        for date in dates:
            c.execute(sql.select_rawdaydata_record, (username, date))
            result = c.fetchone()
            day_json = result[0] if result else None
            day_record = (username, date, day_json)
            mfp_existing_days.append(day_record)
    return mfp_existing_days


@task(name="Load Raw Day Records -> (MyFitnessPaw)")
def mfp_insert_raw_days(days_values: Sequence[Tuple]) -> None:
    """Insert a sequence of day values in the database."""
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(sql.insert_or_replace_rawdaydata_record, days_values)
        conn.commit()


@task(name="Load Notes Records -> (MyFitnessPaw)")
def mfp_insert_notes(notes_values: Sequence[Tuple]) -> None:
    """Insert a sequence of note values in the database."""
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(sql.insert_note_record, notes_values)
        conn.commit()


@task(name="Load Water Records -> (MyFitnessPaw)")
def mfp_insert_water(water_values: Sequence[Tuple]) -> None:
    """Insert a sequence of water records values in the database."""
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(sql.insert_water_record, water_values)
        conn.commit()


@task(name="Load Goals Records -> (MyFitnessPaw)")
def mfp_insert_goals(goals_values: Sequence[Tuple]) -> None:
    """Insert a sequence of goals records in the database."""
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(sql.insert_goals_record, goals_values)
        conn.commit()


@task(name="Load Meal Records -> (MyFitnessPaw)")
def mfp_insert_meals(meals_values: Sequence[Tuple]) -> None:
    """Insert a sequence of meal values in the database."""
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(sql.insert_meal_record, meals_values)
        conn.commit()


@task(name="Load MealEntry Records -> (MyFitnessPaw)")
def mfp_insert_mealentries(mealentries_values: Sequence[Tuple]) -> None:
    """Insert a sequence of meal entry values in the database."""
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(sql.insert_mealentry_record, mealentries_values)
        conn.commit()


@task(name="Load CardioExercises Records -> (MyFitnessPaw)")
def mfp_insert_cardio_exercises(cardio_list: Sequence[Tuple]) -> None:
    """Insert a sequence of cardio exercise entries in the database."""
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(sql.insert_cardioexercises_command, cardio_list)
        conn.commit()


@task(name="Load StrengthExercises Records -> (MyFitnessPaw)")
def mfp_insert_strength_exercises(strength_list: Sequence[Tuple]) -> None:
    """Insert a sequence of strength exercise values in the database."""
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(sql.insert_strengthexercises_command, strength_list)
        conn.commit()


@task(name="Load Measurement Records -> (MyFitnessPaw)")
def mfp_insert_measurements(measurements: Sequence[Tuple]) -> None:
    """Insert a sequence of measurements in the database."""
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.executemany(sql.insert_measurements_command, measurements)
        conn.commit()


@task
def prepare_report_data_for_user(user, usermail: str) -> dict:
    """Return a dictionary containing the data to populate the experimental report."""
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.execute(sql.select_alpha_report_totals, (usermail,))
        report_totals = dict(c.fetchall())

        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        lastweek = yesterday - datetime.timedelta(days=6)
        c.execute(
            sql.select_alpha_report_range,
            (usermail, lastweek.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")),
        )
        report_week = dict(c.fetchall())

        mfp_report_data = {
            "title": f"MyFitnessPaw Scheduled Report for {user}",
            "user": f"{user}",
            "today": datetime.datetime.now().strftime("%d %b %Y"),
            "weekly": {
                "days_total": (
                    "Number of days scraped by MyFitnessPaw",
                    report_week.get("days_total"),
                ),
                "meals_consumed": (
                    "Days where at least one daily meal was tracked",
                    report_week.get("meals_consumed"),
                ),
                "meals_breakfast": (
                    "Days with breakfast",
                    report_week.get("meals_breakfast"),
                ),
                "meals_lunch": (
                    "Days with lunch",
                    report_week.get("meals_lunch"),
                ),
                "meals_dinner": (
                    "Days with dinner",
                    report_week.get("meals_dinner"),
                ),
                "meals_snacks": (
                    "Days with snacks",
                    report_week.get("meals_snacks"),
                ),
                "calories_consumed": (
                    "Total calories consumed (all time)",
                    report_week.get("calories_consumed"),
                ),
                "calories_breakfast": (
                    "Total calories consumed for breakfast (all time)",
                    report_week.get("calories_breakfast"),
                ),
                "calories_lunch": (
                    "Total calories consumed for lunch (all time)",
                    report_week.get("calories_lunch"),
                ),
                "calories_dinner": (
                    "Total calories consumed for dinner (all time)",
                    report_week.get("calories_dinner"),
                ),
                "calories_snacks": (
                    "Total calories consumed for snacks (all time)",
                    report_week.get("calories_snacks"),
                ),
            },
            "user_totals": {
                "days_with_meals": (
                    "Days with at least one meal tracked",
                    report_totals.get("days_with_meals"),
                ),
                "days_with_cardio": (
                    "Days with any cardio exercises tracked",
                    report_totals.get("days_with_cardio"),
                ),
                "days_with_strength": (
                    "Days with any strength exercises tracked",
                    report_totals.get("days_with_strength"),
                ),
                "days_with_measures": (
                    "Days with any measure tracked",
                    report_totals.get("days_with_measures"),
                ),
                "num_meals": (
                    "Total number of tracked meals",
                    report_totals.get("num_entries_meals"),
                ),
                "num_cardio": (
                    "Total number of cardio exercises",
                    report_totals.get("num_entries_meals"),
                ),
                "num_measures": (
                    "Total number of measures taken",
                    report_totals.get("num_entries_meals"),
                ),
                "calories_consumed": (
                    "Total number of calories consumed",
                    report_totals.get("total_calories_consumed"),
                ),
                "calories_exercised": (
                    "Total number of calories burned with exercise",
                    report_totals.get("total_calories_exercised"),
                ),
            },
            "footer": {
                "generated_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        }
        return mfp_report_data


@task
def prepare_report_style_for_user(user: str) -> dict:
    """Return a dictionary containing the values to style the experimental report."""
    mfp_report_style = {
        "title_bg_color": "#fec478",
        "article_bg_color": "#EDF2FF",
    }
    return mfp_report_style


@task
def render_html_email_report(
    template_name: str, report_data: dict, report_style: dict
) -> str:
    """Render a Jinja2 HTML template containing the report to send."""
    template_loader = jinja2.FileSystemLoader(searchpath=TEMPLATES_DIR)
    template_env = jinja2.Environment(loader=template_loader)
    report_template = template_env.get_template(template_name)
    return report_template.render(data=report_data, style=report_style)


@task
def send_email_report(email_addr: str, message: str) -> None:
    """Send a prepared report to the provided address."""
    e = EmailTask(
        subject="MyFitnessPaw Report",
        msg=message,
        email_from="Lisko Reporting Service",
    )
    e.run(email_to=email_addr)


@task
def save_email_report_locally(message: str) -> None:
    "Temporary function to see the result of the render locally."
    with open("temp_report.html", "w") as f:
        f.write(message)


@task(name="Backup MFP Database to Dropbox")
def make_dropbox_backup(
    dbx_token: str,
    dbx_mfp_dir: str,
) -> dropbox.files.FileMetadata:
    """Upload a copy of the database file to the Dropbox backup location."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
    source_path = DB_PATH
    dest_path = f"{dbx_mfp_dir}/mfp_db_backup_{timestamp}"
    dbx = dropbox.Dropbox(dbx_token)
    with open(source_path, "rb") as file:
        res = dbx.files_upload(file.read(), dest_path, mode=WriteMode.overwrite)
    return res


@task(name="List Available Files in Dropbox Backup Directory")
def dbx_list_available_backups(
    dbx_token: str,
    dbx_mfp_dir: str,
) -> List[str]:
    """Query the Dropbox backup directory for available backup files."""
    dbx = dropbox.Dropbox(dbx_token)
    res = dbx.files_list_folder(dbx_mfp_dir)
    return [f.name for f in res.entries]


@task(name="Apply Backup Rotation Scheme")
def apply_backup_rotation_scheme(
    dbx_token: str,
    dbx_mfp_dir: str,
    files_list: Sequence,
) -> List[Tuple[Any, Any]]:
    """Apply the current backup rotation scheme (FIFO) to a provided file list."""
    #  hardcoding it to keep only the most recent 5 for now in order to see how it works.
    files_to_delete = select_fifo_backups_to_delete(5, files_list)
    dbx = dropbox.Dropbox(dbx_token)
    if not files_to_delete:
        return []
    deleted = []
    for filename in files_to_delete:
        res = dbx.files_delete(f"{dbx_mfp_dir}/{filename}")
        deleted.append((res.name, res.content_hash))
    return deleted
