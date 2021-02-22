import datetime
import sqlite3
from contextlib import closing
from datetime import timedelta
from typing import Any, List, Sequence, Tuple, Union, cast

import dropbox
import jinja2
import jsonpickle
import prefect
from dropbox.files import WriteMode
from myfitnesspal.meal import Meal
from prefect import Task, task
from prefect.tasks.notifications.email_task import EmailTask
from prefect.utilities.tasks import defaults_from_attrs

from . import DB_PATH, TEMPLATES_DIR, sql
from ._utils import (
    MaterializedDay,
    MyfitnesspalClientAdapter,
    select_fifo_backups_to_delete,
    try_parse_date_str,
)


class SQLiteExecuteMany(Task):
    """
    Task for executing many queries against a SQLite file database.

    Args:
      - db (str, optional): the location of the database file.
      - query (str, optional): the optional _default_ query to execute at runtime;
        can also be provided as a keyword to `run`, which takes precedence over this
        default.
      - data (List[Tuple]): list of values to be used with the query.
      - enforce_fk (bool, optional): SQLite does not enforce foreign key constraints by
        default (see https://sqlite.org/foreignkeys.html). In order to enable foreign key
        support, an additional `PRAGMA foreign_keys = YES;` statement is executed before
        the main task statement. TODO: There is an additional case where the sqlite
        database has been compiled without FK support which is not yet handled by this
        task.
      - **kwargs (optional): additional keyword arguments to pass to the standard
        Task initialization.
    """

    def __init__(
        self,
        db: str = None,
        query: str = None,
        data: List[tuple] = None,
        enforce_fk: bool = None,
        **kwargs: Any,
    ):
        self.db = db
        self.query = query
        self.data = data
        self.enforce_fk = enforce_fk
        super().__init__(**kwargs)

    @defaults_from_attrs("db", "query", "data", "enforce_fk")
    def run(
        self,
        db: str = None,
        query: str = None,
        data: list = None,
        enforce_fk: bool = None,
    ):
        """
        Task run method. Executes many queries against a SQLite file database.
        Args:
            - db (str, optional):
            - query (str, optional): query to execute against database.
            - data (List[tuple], optional): list of values to use in the query.
            - enforce_fk (bool, optional): SQLite does not enforce foreign
              key constraints by default. To force the query to cascade delete
              for example set the fk_constraint to True.
        Returns:
            - None
        Raises:
            - ValueError: if `query` is None or empty string.
            - ValueError: if `db` is not provided at either initialization or runtime.
            - ValueError: if `data` is not provided at either initialization or runtime.
              Passing an empty list as `data` is allowed.
        """
        if not db:
            raise ValueError("A databse connection string must be provided.")

        if not query:
            raise ValueError("A query string must be provided.")

        if data is None:  # allow empty lists to proceed,
            raise ValueError("A data list must be provided.")

        db = cast(str, db)
        query = cast(str, query)
        with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
            if enforce_fk is True:
                cursor.execute("PRAGMA foreign_keys = YES;")
            cursor.executemany(query, data)
            conn.commit()


@task
def prepare_extraction_start_end_dates(
    from_date_str: Union[str, None], to_date_str: Union[str, None]
) -> Tuple[datetime.date, datetime.date]:
    """
    Returns the start and end date as boundaries of the period to be extracted.

    With provided both from_date and to_date, the task returns the parsed dates that
    encompass the period to be extracted from myfitnesspal (including both the start and
    end dates). If the arguments are not provided, the dates returned will specify the
    default 5 day scan period (from 6 days ago to yesterday).

    Args:
        - from_date_str (str): The date on which to start the extraction period.
        - to_date_str (str): The date on which to end the extraction period.
    Returns:
        - Tuple[datetime.date, datetime.date]: The start and end dates of the extraction
          period, parsed.
    Raises:
        - ValueError: if only one of the dates is provided.
    """
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


@task
def generate_dates_to_extract(
    from_date: datetime.date, to_date: datetime.date
) -> List[datetime.date]:
    """
    Return a list of dates between from_date and to_date to be extracted.

    Args:
        - from_date (datetime.date): The starting date to generate from.
        - to_date (datetime.date): The ending date to generate to.
    Returns:
        - List[datetime.date]: A list of dates to be extracted from myfitnesspal.
    Raises:
        - ValueError: if to_date is before from_date.
    """
    if from_date > to_date:
        raise ValueError("Parameter to_date cannot be before from_date.")
    delta_days = (to_date - from_date).days
    #  including both the starting and ending date
    return [from_date + timedelta(days=i) for i in range(delta_days + 1)]


@task
def create_mfp_database() -> None:
    """
    Create the MyFitnessPaw project sqlite database tables.

    This task executes a series of commands to build the project database tables.
    All commands include the CREATE TABLE IF NOT EXISTS clause, making this function
    safe to run against an already existing database, thus making the task idempotent.

    Returns:
        - None
    """
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


@task(timeout=15, max_retries=5, retry_delay=timedelta(seconds=5))
def get_myfitnesspal_day(
    username: str, password: str, date: datetime.date, measures: List[str]
) -> MaterializedDay:
    """
    Get the myfitnesspal data associated with the given date.

    Extracts the myfitnesspal data for the date which includes all food, exercise, notes,
    water, and also the measurements for the provided measures list for the given date.

    Args:
        - username (str): The username for the myfitnesspal account.
        - password (str): The password associated with the provided username.
        - measures (List[str]): A list of measures to be collected.
    Returns:
        - MaterializedDay: Containing the extracted information.
    """
    with MyfitnesspalClientAdapter(username, password) as myfitnesspal:
        day = myfitnesspal.get_myfitnesspaw_day(date, measures)
    return day


@task
def serialize_myfitnesspal_days(
    myfitnesspal_days: Sequence,
) -> List[Tuple[str, datetime.date, str]]:
    """Prepare a list of serialized Day records."""
    days_values = [
        (day.username, day.date, jsonpickle.encode(day)) for day in myfitnesspal_days
    ]
    return days_values


@task
def filter_new_or_changed_records(
    extracted_records: Sequence[str], local_records: Sequence[str]
) -> List[str]:
    """Filter out extracted records that are locally available already."""
    logger = prefect.context.get("logger")
    records_to_upsert = [t for t in extracted_records if t not in local_records]
    logger.info(f"Records to Insert/Update: {len(records_to_upsert)}")
    return records_to_upsert


@task
def deserialize_records_to_process(
    serialized_days: Sequence[str],
) -> List[MaterializedDay]:
    """Deserialize a sequence of days."""
    result = []
    for day_json in serialized_days:
        day_decoded = jsonpickle.decode(day_json[2], classes=[MaterializedDay])
        result.append(day_decoded)
    return result


@task
def extract_notes(days: Sequence[MaterializedDay]) -> List[Tuple]:
    """Extract myfitnesspal Food Notes values from a sequence of myfitnesspal days."""
    return [
        (
            day.username,
            day.date,
            day.notes.get("type"),
            day.notes.get("body"),
        )
        for day in days
        if day.notes["body"]
    ]


@task
def extract_water(days: Sequence[MaterializedDay]) -> List[Tuple]:
    """Extract myfitnesspal water values from a sequence of myfitnesspal days."""
    return [(day.username, day.date, day.water) for day in days]


@task
def extract_goals(days: Sequence[MaterializedDay]) -> List[Tuple]:
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


@task
def extract_meals(days: Sequence[MaterializedDay]) -> List[Meal]:
    """Extract myfitnesspal Meal items from a sequence of myfitnesspal Days."""
    for day in days:
        for meal in day.meals:
            if not meal:  # TODO: ?
                continue
            meal.username = day.username
            meal.date = day.date
    return [meal for day in days for meal in day.meals if meal]


@task
def extract_meal_records(meals: Sequence[Meal]) -> List[Tuple]:
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


@task
def extract_mealentries(meals: Sequence[Meal]) -> List[Tuple]:
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


@task
def extract_cardio_exercises(days: Sequence[MaterializedDay]) -> List[Tuple]:
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


@task
def extract_strength_exercises(
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


@task
def extract_measures(days: Sequence[MaterializedDay]) -> List[Tuple]:
    """Extract measures values from a sequence of myfitnesspal days."""
    return [
        (
            day.username,
            day.date,
            measure_name,
            measure_value,
        )
        for day in days
        for measure_name, measure_value in day.measurements.items()
    ]


@task
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


@task
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


@task
def dbx_list_available_backups(
    dbx_token: str,
    dbx_mfp_dir: str,
) -> List[str]:
    """Query the Dropbox backup directory for available backup files."""
    dbx = dropbox.Dropbox(dbx_token)
    res = dbx.files_list_folder(dbx_mfp_dir)
    return [f.name for f in res.entries]


@task
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
