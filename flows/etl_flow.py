"""Myfitnesspaw's Extract-Transform-Load Prefect Flow.

This Prefect flow encompases the steps to extract information from www.myfitnesspal.com,
transform and store it locally in an SQLite database. The raw objects' data is stored in
a serialized JSON form which is then used to prepare several report-friendly tables.
"""
import datetime
from datetime import timedelta

import jsonpickle
import myfitnesspal
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
    name="<MyFitnessPaw>: Create DB (if not existing)",
    db="database/mfp_db.sqlite",
    script=create_mfp_database_script,
)


@task(name="Prepare list of dates to scrape")
def prepare_list_of_dates_to_scrape(from_date, to_date):
    delta_days = (to_date - from_date).days
    #  including both the starting and ending date
    return [from_date + timedelta(days=i) for i in range(delta_days + 1)]


@task(
    name="Get day record json from date (myfitnesspal)",
    timeout=5,
    max_retries=10,
    retry_delay=timedelta(seconds=10),
)
def get_myfitnesspal_day_serialized(username, password, date):
    client = myfitnesspal.Client(username=username, password=password)
    day = client.get_date(date)
    day.username = username
    day._username = username
    day._exercises = day.exercises
    day._water = day.water
    day._totals = day.totals
    day._notes = day.notes if day.notes else ""
    day_json = jsonpickle.encode(day)
    return (username, date, day_json)


with Flow("Myfitnesspal Prototype Flow") as flow:
    #  Gather required parameters/secrets
    from_date = Parameter(name="from_date", required=True)
    to_date = Parameter(name="to_date", required=True)
    username = EnvVarSecret("MYFITNESSPAW_USERNAME", raise_if_missing=True)
    password = EnvVarSecret("MYFITNESSPAW_PASSWORD", raise_if_missing=True)

    #  Prepeare a list of dates to be scraped:
    # TODO: Should this really be a Task? Or just construct a list comprehension
    dates_to_scrape = prepare_list_of_dates_to_scrape(
        from_date, to_date, upstream_tasks=[create_mfp_database]
    )

    #  Get a serialized day record for each date in the list:
    mfp_raw_days_data = get_myfitnesspal_day_serialized.map(
        date=dates_to_scrape,
        username=unmapped(username),
        password=unmapped(password),
    )

if __name__ == "__main__":
    flow.run(
        from_date=datetime.date(2020, 9, 10),
        to_date=datetime.date(2020, 9, 10),
    )
