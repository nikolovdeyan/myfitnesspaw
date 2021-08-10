"""
myfitnesspaw.tasks

Contains all tasks used in this project's flows.
"""

import datetime
import os
import smtplib
import sqlite3
import ssl
from contextlib import closing
from datetime import timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, List, Tuple, Union, cast

import dropbox
import jinja2
import jsonpickle
import matplotlib.pyplot as plt
import numpy as np
import prefect
from dropbox.files import WriteMode
from myfitnesspal.meal import Meal
from prefect import Task, task
from prefect.client import Secret
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

    This class mimics Prefect's implementation of `PostgresExecuteMany`, except:
      - `commit` parameter is not implemented. Transaction is executed immediately.
    TODO: Fix that.

    Args:
      - db (str, optional): the location of the database file
      - query (str, optional): the optional _default_ query to execute at runtime;
        can also be provided as a keyword to `run`, which takes precedence over this
        default
      - data ([Tuple]): list of values to be used with the query
      - enforce_fk (bool, optional): SQLite does not enforce foreign key constraints by
        default (see https://sqlite.org/foreignkeys.html). In order to enable foreign key
        support, an additional `PRAGMA foreign_keys = YES;` statement is executed before
        the main task statement. TODO: There is an additional case where the sqlite
        database has been compiled without FK support which is not yet handled by this
        task
      - **kwargs (optional): additional keyword arguments to pass to the standard
        Task initialization

    Returns:
      - None

    Raises:
      - ValueError: if query parameter is None or a blank string
      - DatabaseError: if exception occurs when executing the query
    """

    def __init__(
        self,
        db: str = None,
        query: str = None,
        data: List[Tuple] = None,
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
           - data (List[tuple], optional): list of values to use in the query
           - enforce_fk (bool, optional): SQLite does not enforce foreign
             key constraints by default. To force the query to cascade delete
             for example set the fk_constraint to True

        Returns:
           - None

        Raises:
           - ValueError: if `query` is None or empty string
           - ValueError: if `db` is not provided at either initialization or runtime
           - ValueError: if `data` is not provided at either initialization or runtime
             Note: Passing an empty list as `data` is allowed.
        """

        if not db:
            raise ValueError("A database connection string must be provided")

        if not query:
            raise ValueError("A query string must be provided")

        if data is None:  # allow empty lists to proceed,
            raise ValueError("A data list must be provided")

        db = cast(str, db)
        query = cast(str, query)
        with closing(sqlite3.connect(db)) as conn, closing(conn.cursor()) as cursor:
            if enforce_fk:
                cursor.execute("PRAGMA foreign_keys = YES;")
            cursor.executemany(query, data)
            conn.commit()


class LiskoEmailTask(Task):
    def __init__(
        self,
        subject: str = None,
        msg: str = None,
        email_to: str = None,
        email_from: str = "notifications@prefect.io",
        smtp_server: str = "smtp.gmail.com",
        smtp_port: int = 465,
        smtp_type: str = "SSL",
        msg_plain: str = None,
        email_to_cc: str = None,
        email_to_bcc: str = None,
        attachments: List[str] = None,
        **kwargs: Any,
    ):
        self.subject = subject
        self.msg = msg
        self.email_to = email_to
        self.email_from = email_from
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_type = smtp_type
        self.msg_plain = msg_plain
        self.email_to_cc = email_to_cc
        self.email_to_bcc = email_to_bcc
        self.attachments = attachments or []
        super().__init__(**kwargs)

    @defaults_from_attrs(
        "subject",
        "msg",
        "email_to",
        "email_from",
        "smtp_server",
        "smtp_port",
        "smtp_type",
        "msg_plain",
        "email_to_cc",
        "email_to_bcc",
        "attachments",
    )
    def run(
        self,
        subject: str = None,
        msg: str = None,
        email_to: str = None,
        email_from: str = None,
        smtp_server: str = None,
        smtp_port: int = None,
        smtp_type: str = None,
        msg_plain: str = None,
        email_to_cc: str = None,
        email_to_bcc: str = None,
        attachments: List[str] = None,
    ) -> None:
        username = cast(str, Secret("EMAIL_USERNAME").get())
        password = cast(str, Secret("EMAIL_PASSWORD").get())

        message = MIMEMultipart()
        message["Subject"] = subject
        message["From"] = email_from
        message["To"] = email_to
        if email_to_cc:
            message["Cc"] = email_to_cc
        if email_to_bcc:
            message["Bcc"] = email_to_bcc

        # First add the message in plain text, then the HTML version. Email clients try to render
        # the last part first
        if msg_plain:
            message.attach(MIMEText(msg_plain, "plain"))
        if msg:
            message.attach(MIMEText(msg, "html"))

        for filepath in attachments:
            with open(filepath, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())

            encoders.encode_base64(part)
            filename = os.path.basename(filepath)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename}",
            )
            part.add_header("Content-ID", "<test.png@lisko.id>")
            message.attach(part)

        context = ssl.create_default_context()
        if smtp_type == "SSL":
            server = smtplib.SMTP_SSL(smtp_server, smtp_port, context=context)
        elif smtp_type == "STARTTLS":
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls(context=context)
        else:
            raise ValueError(f"{smtp_type} is an unsupported value for smtp_type.")

        server.login(username, password)
        try:
            server.send_message(message)
        finally:
            server.quit()


@task
def prepare_extraction_start_end_dates(
    from_date_str: Union[str, None], to_date_str: Union[str, None]
) -> Tuple[datetime.date, datetime.date]:
    """
    Returns the start and end date as boundaries of the period to be extracted.

    With provided both from_date and to_date, the task returns the parsed dates that
    encompass the period to be extracted from myfitnesspal (including both the start and
    the end date. If the arguments are not provided, the dates returned will specify the
    default 5 day extraction period (from 6 days ago to yesterday).

    Args:
       - from_date_str (str): The date on which to start the extraction period
       - to_date_str (str): The date on which to end the extraction period

    Returns:
       - Tuple[datetime.date, datetime.date]: The start and end dates of the extraction
         period, parsed

    Raises:
       - ValueError: if only one of the dates is provided
       - ValueError: if incorrect date format is provided
    """
    today = datetime.date.today()
    default_from_date = today - timedelta(days=6)
    default_to_date = today - timedelta(days=1)

    if (from_date_str is not None and to_date_str is None) or (
        from_date_str is None and to_date_str is not None
    ):
        raise ValueError(
            "Either both from_date and to_date should be provided or neither"
        )

    if from_date_str is None:
        from_date = default_from_date
    else:
        from_date = try_parse_date_str(from_date_str).date()
    if to_date_str is None:
        to_date = default_to_date
    else:
        to_date = try_parse_date_str(to_date_str).date()

    return from_date, to_date


@task
def generate_dates_to_extract(
    from_date: datetime.date, to_date: datetime.date
) -> List[datetime.date]:
    """
    Return a list of dates between from_date and to_date to be extracted.

    Args:
        - from_date (datetime.date): The starting date of the extraction period
        - to_date (datetime.date): The ending date of the extraction period

    Returns:
        - List[datetime.date]: A list of dates to have data extracted for from myfitnesspal

    Raises:
        - ValueError: if to_date is before from_date
    """

    if from_date > to_date:
        raise ValueError("Parameter to_date cannot be before from_date")

    delta_days = (to_date - from_date).days

    #  including both the starting and ending dates:
    return [from_date + timedelta(days=i) for i in range(delta_days + 1)]


@task
def create_mfp_database() -> None:
    """
    Create the MyFitnessPaw project sqlite database schema.

    This task executes a series of commands to build the project database tables.
    All tables are created using the CREATE TABLE IF NOT EXISTS clause, making this
    function safe to run against an already existing database, thus making the
    task idempotent.

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


@task(timeout=15, max_retries=5, retry_delay=timedelta(seconds=15))
def get_myfitnesspal_day(
    username: str, password: str, date: datetime.date, measures: List[str]
) -> MaterializedDay:
    """
    Get the myfitnesspal data associated with the given date.

    Extracts the myfitnesspal data for a date, which includes all food, exercise, notes,
    water, and also the measurements for the provided measures list for the given date.

    Args:
       - username (str): The username for the myfitnesspal account
       - password (str): The password associated with the provided username
       - date (datetime.date): The date to extract data for
       - measures (List[str]): A list of measures to be collected

    Returns:
       - MaterializedDay: Containing the extracted information
    """

    with MyfitnesspalClientAdapter(username, password) as myfitnesspal:
        day = myfitnesspal.get_myfitnesspaw_day(date, measures)

    return day


@task
def serialize_myfitnesspal_days(
    myfitnesspal_days: List[MaterializedDay],
) -> List[Tuple[str, datetime.date, str]]:
    """
    Prepare a list of serialized day records.

    Args:
      - myfitnesspal_days (List[MaterializedDay]): A list of day objects to be serialized

    Returns:
      - List[Tuple[str, datetime.date, str]]: A list of serialized day objects
    """
    return [
        (day.username, day.date, jsonpickle.encode(day)) for day in myfitnesspal_days
    ]


@task
def filter_new_or_changed_records(
    extracted_records: List[str], local_records: List[str]
) -> List[str]:
    """
    Filter out extracted records that are available and unchanged.

    Any information that had been changed in myfitnesspal will produce a mismatch when
    compared against the available copy. All other records that are locally available
    are discarded from the list.

    Args:
      - extracted_records (List[str]): The list with the newly extracted records
      - local_records (List[str]): The list with the locally available records

    Returns:
      - List[str]: A list with new or changed day objects
    """
    logger = prefect.context.get("logger")
    records_to_upsert = [t for t in extracted_records if t not in local_records]
    logger.info(f"Records to Insert/Update: {len(records_to_upsert)}")

    return records_to_upsert


@task
def deserialize_records_to_process(
    serialized_days: List[str],
) -> List[MaterializedDay]:
    """
    Deserialize a sequence of days.

    Args:
      - serialized_days (List[str]): A list containing the serialized days to be
        converted back to `MaterializedDay` objects

    Returns:
      - List[MaterializedDay]: A list with deserialized day objects
    """

    result = []
    for day_json in serialized_days:
        day_decoded = jsonpickle.decode(day_json[2], classes=[MaterializedDay])
        result.append(day_decoded)

    return result


@task
def extract_notes(days: List[MaterializedDay]) -> List[Tuple]:
    """
    Extract myfitnesspal food note values from a list of myfitnesspal days.

    Args:
      - days (List[MaterializedDay]): A list containing the days to extract data from

    Returns:
      - List[Tuple]: A list with notes' values
    """

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
def extract_water(days: List[MaterializedDay]) -> List[Tuple]:
    """
    Extract myfitnesspal water values from a list of myfitnesspal days.

    Args:
      - days (List[MaterializedDay]): A list containing the days to extract data from

    Returns:
      - List[Tuple]: A list with water record values
    """

    return [(day.username, day.date, day.water) for day in days]


@task
def extract_goals(days: List[MaterializedDay]) -> List[Tuple]:
    """
    Extract myfitnesspal daily goals from a sequence of myfitnesspal days.

    Args:
      - days (List[MaterializedDay]): A list containing the days to extract data from

    Returns:
      - List[Tuple]: A list with goal record values
    """

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
def extract_meals(days: List[MaterializedDay]) -> List[Meal]:
    """
    Extract myfitnesspal neal items from a sequence of myfitnesspal days.

    Args:
      - days (List[MaterializedDay]): A list containing the days to extract data from

    Returns:
      - List[Meal]: A list with meal record values
    """

    for day in days:
        for meal in day.meals:
            if not meal:  # TODO: ?
                continue
            meal.username = day.username
            meal.date = day.date

    return [meal for day in days for meal in day.meals if meal]


@task
def extract_meal_records(meals: List[Meal]) -> List[Tuple]:
    """
    Extract meal entry records from a sequence of myfitnesspal meals.

    Args:
      - meals (List[Meal]): A list with meal objects to extract data from

    Returns:
      - List[Tuple]: A list with meal record values
    """

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
def extract_mealentries(meals: List[Meal]) -> List[Tuple]:
    """
    Extract meal entries records from a sequence of myfitnesspal meals.

    Args:
      - meals (List[Meal]): A list with meal objects to extract data from

    Returns:
      - List[Tuple]: A list with meal record values
    """

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
def extract_cardio_exercises(days: List[MaterializedDay]) -> List[Tuple]:
    """
    Extract cardio exercise entries from a sequence of myfitnesspal days.

    Args:
      - days (List[MaterializedDay]): A list containing the days to extract data from

    Returns:
      - List[Tuple]: A list with cardio exercise record values
    """

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
    days: List[MaterializedDay],
) -> List[Tuple]:
    """
    Extract strength exercise entries from a sequence of myfitnesspal days.

    Args:
      - days (List[MaterializedDay]): A list containing the days to extract data from

    Returns:
      - List[Tuple]: A list with strength exercise record values
    """

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
def extract_measures(days: List[MaterializedDay]) -> List[Tuple]:
    """
    Extract measures values from a sequence of myfitnesspal days.

    Args:
      - days (List[MaterializedDay]): A list containing the days to extract data from

    Returns:
      - List[Tuple]: A list with the extracted values
    """

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
    username: str, dates: List[datetime.date]
) -> List[Tuple[str, datetime.date, str]]:
    """
    Select raw day entries for username and provided dates.

    Args:
      - username (str): The username to select
      - dates (List[datetime.date]): A list with dates to select

    Returns:
      - List[Tuple]: A list containing the serialized days selected
    """

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
def mfp_select_daily_report_data(
    user: str,
    usermail: str,
    starting_date: str,
    end_goal: int,
    report_tbl_span_limit: int = 7,
) -> dict:
    """
    Return a dictionary containing the data to populate the experimental report.
    """
    with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.execute(sql.select_daily_report, (usermail, starting_date, end_goal))
        report_data = c.fetchall()

    logger = prefect.context.get("logger")
    report_window_data = [row for row in report_data if row[4] is not None]
    logger.info(f"report_window_data: {report_window_data}")
    yesterday_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%d-%b-%Y"
    )

    # if report starts from today or yesterday has no entered info, return None
    if not report_window_data or report_window_data[-1][1] != yesterday_str:
        return {}

    nutrition_tbl_header = [
        "day",
        "date",
        "cal target",
        "deficit target",
        "deficit actual",
        "running deficit",
    ]
    yesterday_tbl_row = report_window_data[-1]
    chart_data = calculate_chart_data(end_goal, yesterday_tbl_row)
    highlight_row_data = None
    current_day_number = yesterday_tbl_row[0]
    nutrition_tbl_data = report_window_data[(report_tbl_span_limit * -1) :]

    return {
        "subject": f"MyfitnessPaw Daily Report (Day {current_day_number})",
        "title": f"MyFitnessPaw Daily Report (Day {current_day_number})",
        "user": f"{user}".capitalize(),
        "today": datetime.datetime.now().strftime("%d %b %Y"),
        "current_day_number": current_day_number,
        "nutrition_tbl_header": nutrition_tbl_header,
        "nutrition_tbl_data": nutrition_tbl_data,
        "chart_data": chart_data,
        "highlight_row_data": highlight_row_data,
        "footer": {
            "generated_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    }


@task
def prepare_report_style(style_name: str) -> dict:
    """Return a dictionary containing the values to style the experimental report."""
    available_styles = {
        "blues": {
            "title_bg_color": "#fec478",
            "article_bg_color": "#EDF2FF",
        },
        "lisk": {
            "title_bg_color": "#fe8821",
            "article_bg_color": "#feecd3",
            "bg-1": "#FEECD3",
            "bg0": "#FEDBAB",
            "bg+1": "#FEDBAB",
            "fg-1": "#FFB967",
            "fg0": "#FE8821",
            "fg+1": "",
            "text-1": "",
            "text0": "#3C3A41",
            "text+1": "",
            "accent": "#21D8FF",
            "faded": "#958476",
            "faded-1": "#CCBBAD",
            "warning": "#FF3D14",
        },
    }
    return available_styles.get(style_name)


@task
def prepare_report_chart(report_data, report_style):
    chart_data = report_data.get("chart_data", None)
    col = list(chart_data.values())[0][1]
    vals = tuple(chart_data.values())[0][0]
    category_colors = [
        report_style.get("faded", "green"),
        report_style.get(col, None),
        report_style.get("faded-1", "lightgray"),
    ]
    labels = list(chart_data.keys())
    data = np.array(list(vals))
    data_cum = data.cumsum()
    fig = plt.figure(figsize=(5.5, 0.7))
    ax = fig.add_subplot(111)

    fig.set_facecolor("#00000000")
    ax.set_axis_off()
    ax.set_ymargin(0.5)
    ax.set_xlim(0, np.sum(data, axis=0).max())
    goals_bar = ax.barh(  # noqa
        labels,
        width=data,
        left=data_cum[:] - data,
        color=category_colors,
    )

    our_dir = Path().absolute()
    chart_dir = our_dir.joinpath(Path("tmp"))
    chart_dir.mkdir(exist_ok=True)
    chart_file = chart_dir.joinpath(Path("temp.png"))

    plt.savefig(chart_file)
    return chart_file


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
def prepare_report_subject(report_data):
    return report_data.get("subject", None)


@task
def send_email_report(email_addr: str, subject: str, message: str, attachments) -> None:
    """Send a prepared report to the provided address."""
    e = LiskoEmailTask(
        subject=subject,
        msg=message,
        email_from="Lisko Home Automation",
        attachments=attachments,
    )

    e.run(email_to=email_addr)


@task
def save_email_report_locally(message: str) -> None:
    """Temporary function to see the result of the render locally."""
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
    files_list: List,
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


def calculate_chart_data(end_goal, report_data):
    current_date = report_data[1]
    deficit_actual = report_data[4]
    deficit_accumulated = report_data[5]

    if deficit_actual < 0:
        deficit_remaining = end_goal - deficit_accumulated + abs(deficit_actual)
        current_date_data = (
            (
                deficit_accumulated - abs(deficit_actual),
                abs(deficit_actual),
                deficit_remaining + deficit_actual,
            ),
            "warning",
        )

    else:
        deficit_remaining = end_goal - deficit_accumulated - deficit_actual
        current_date_data = (
            (
                deficit_accumulated,
                deficit_actual,
                deficit_remaining,
            ),
            "accent",
        )

    chart_data = {current_date: current_date_data}
    return chart_data
