"""MyFitnessPaw's Extract-Transform-Load Prefect Flow.

This Prefect flow encompases the steps to extract information from www.myfitnesspal.com,
transform and store it locally in an SQLite database. The raw objects' data is stored in
a serialized JSON form which is then used to prepare several report-friendly tables.
"""
import datetime
import sqlite3
from contextlib import closing
from datetime import timedelta

import jsonpickle
import myfitnesspal
import prefect
import sql
from prefect import Flow, Parameter, task, unmapped
from prefect.tasks.database.sqlite import SQLiteScript
from prefect.tasks.secrets import EnvVarSecret

create_mfp_database_script = f"""
{sql.create_raw_day_table}

{sql.create_meals_table}

{sql.create_mealentries_table}

{sql.create_cardioexercises_table}

{sql.create_strengthexercises_table}

{sql.create_measurements_table}
"""

create_mfp_database = SQLiteScript(
    name="Create MyFitnessPaw DB (if not existing)",
    db="database/mfp_db.sqlite",
    script=create_mfp_database_script,
)


@task(name="Prepare Dates Sequence to Extract")
def generate_dates_to_extract(from_date, to_date):
    delta_days = (to_date - from_date).days
    #  including both the starting and ending date
    return [from_date + timedelta(days=i) for i in range(delta_days + 1)]


@task(
    name="Get Day Record for Date <- (myfitnesspal)",
    timeout=5,
    max_retries=10,
    retry_delay=timedelta(seconds=10),
)
def get_myfitnesspal_day(username, password, date):
    client = myfitnesspal.Client(username=username, password=password)
    day = client.get_date(date)
    day._username = username  # add username to the day object
    day._exercises = day.exercises
    day._water = day.water
    day._notes = day.notes if day.notes else ""
    return day


@task(name="Serialize Day Records List")
def serialize_myfitnesspal_days(myfitnesspal_days):
    """Prepare a list of Day records for database load."""
    days_values = [
        (day._username, day._date, jsonpickle.encode(day)) for day in myfitnesspal_days
    ]
    return days_values


@task(name="Filter New or Changed Day Records")
def filter_records_to_upsert(extracted_records, local_records):
    logger = prefect.context.get("logger")
    records_to_upsert = [t for t in extracted_records if t not in local_records]
    logger.info(f"Records to Insert/Update: {len(records_to_upsert)}")
    return records_to_upsert


@task(name="Get Raw Day Records for Dates <- (MyFitnessPaw)")
def mfp_select_raw_days(username, dates):
    mfp_existing_days = []
    with closing(sqlite3.connect("database/mfp_db.sqlite")) as conn, closing(
        conn.cursor()
    ) as cursor:
        for date in dates:
            cursor.execute(sql.select_rawdaydata_record, (username, date))
            result = cursor.fetchone()
            day_json = result[0] if result else None
            day_record = (username, date, day_json)
            mfp_existing_days.append(day_record)
    return mfp_existing_days


@task(name="Load Raw Day Records -> (MyFitnessPaw)")
def mfp_insert_raw_days(days_values):
    with closing(sqlite3.connect("database/mfp_db.sqlite")) as conn, closing(
        conn.cursor()
    ) as cursor:
        cursor.executemany(sql.insert_or_replace_rawdaydata_record, days_values)
        conn.commit()


with Flow("MyFitnessPaw ETL Flow") as flow:
    #  Gather required parameters/secrets
    from_date = Parameter(
        name="from_date",
        required=False,
        default=datetime.date.today() - timedelta(days=1),
    )
    to_date = Parameter(
        name="to_date",
        required=False,
        default=datetime.date.today() - timedelta(days=1),
    )
    username = EnvVarSecret("MYFITNESSPAW_USERNAME", raise_if_missing=True)
    password = EnvVarSecret("MYFITNESSPAW_PASSWORD", raise_if_missing=True)

    #  Prepeare a sequence of dates to be scraped:
    dates_to_extract = generate_dates_to_extract(from_date, to_date)

    #  Get a myfitnesspal Day record for each date in the list:
    extracted_days = get_myfitnesspal_day.map(
        date=dates_to_extract,
        username=unmapped(username),
        password=unmapped(password),
    )

    #  Prepare the extracted days for load to mfp database:
    serialized_extracted_days = serialize_myfitnesspal_days(extracted_days)

    #  We need to compare the extracted days with what we already have in the database
    #  for this username and date. Select the records applicable:
    mfp_existing_days = mfp_select_raw_days(
        username=username, dates=dates_to_extract, upstream_tasks=[create_mfp_database]
    )

    #  Compare the existing records with the ones just scraped:
    #  This can probably also be realized with FilterTask:
    days_to_upsert = filter_records_to_upsert(
        extracted_records=serialized_extracted_days,
        local_records=mfp_existing_days,
    )

    #  Load the transformed sequence of raw myfitnesspal days to mfp database:
    days_load_state = mfp_insert_raw_days(days_to_upsert)


if __name__ == "__main__":
    flow.visualize(filename="mfp_etl_dag", format="png")
    # flow.register(project_name="MFP Prototype")
    flow_state = flow.run(
        from_date=datetime.date(2020, 9, 9),
        to_date=datetime.date(2020, 9, 10),
    )
    # flow.visualize(
    #     flow_state=flow_state,
    #     filename="mfp_etl_flow_status",
    #     format="png"
    # )
