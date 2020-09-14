"""Myfitnesspaw's Extract-Transform-Load Prefect Flow.

This Prefect flow encompases the steps to extract information from www.myfitnesspal.com,
transform and store it locally in an SQLite database. The raw objects' data is stored in
a serialized JSON form which is then used to prepare several report-friendly tables.
"""
import datetime
from datetime import timedelta

import jsonpickle
import myfitnesspal
from prefect import Flow, Parameter, task, unmapped
from prefect.tasks.database.sqlite import SQLiteScript

create_raw_day_table_command = """
CREATE TABLE IF NOT EXISTS RawDayData (
  userid text NOT NULL,
  date text NOT NULL,
  rawdaydata json,
  PRIMARY KEY(userid, date)
);
"""

create_meals_table_command = """
CREATE TABLE IF NOT EXISTS Meals (
  userid text NOT NULL,
  date text NOT NULL,
  name text NOT NULL,
  calories INTEGER,
  carbs INTEGER,
  fat INTEGER,
  protein INTEGER,
  sodium INTEGER,
  sugar INTEGER,
  PRIMARY KEY(userid, date, name),
  CONSTRAINT fk_rawdaydata
    FOREIGN KEY (userid, date)
    REFERENCES RawDayData(userid, date)
    ON DELETE CASCADE
);
"""

create_mealentries_table_command = """
CREATE TABLE IF NOT EXISTS MealEntries (
  id integer PRIMARY KEY AUTOINCREMENT,
  userid text NOT NULL,
  date text NOT NULL,
  meal_name text NOT NULL,
  short_name text NOT NULL,
  quantity REAL NOT NULL,
  unit text NOT NULL,
  calories INTEGER,
  carbs INTEGER,
  fat INTEGER,
  protein INTEGER,
  sodium INTEGER,
  sugar INTEGER,
  CONSTRAINT fk_mealentries
    FOREIGN KEY(userid, date, meal_name)
    REFERENCES Meals(userid, date, name)
    ON DELETE CASCADE
);
"""

create_cardioexercises_table_command = """
CREATE TABLE IF NOT EXISTS CardioExercises (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  userid TEXT NOT NULL,
  date TEXT NOT NULL,
  exercise_name TEXT NOT NULL,
  minutes REAL,
  calories_burned REAL,
  CONSTRAINT fk_rawdaydata
    FOREIGN KEY(userid, date)
    REFERENCES RawDayData(userid, date)
    ON DELETE CASCADE
);
"""

create_strengthexercises_table_command = """
CREATE TABLE IF NOT EXISTS StrengthExercises (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  userid TEXT NOT NULL,
  date TEXT NOT NULL,
  exercise_name TEXT NOT NULL,
  sets REAL,
  reps REAL,
  weight REAL,
  CONSTRAINT fk_rawdaydata
    FOREIGN KEY(userid, date)
    REFERENCES RawDayData(userid, date)
    ON DELETE CASCADE
);
"""

create_measurements_table_command = """
CREATE TABLE IF NOT EXISTS Measurements (
  userid text NOT NULL,
  date text NOT NULL,
  measure_name text NOT NULL,
  value REAL NOT NULL,
  PRIMARY KEY(userid, date, measure_name)
);
"""

create_mfp_database_script = f"""
{create_raw_day_table_command}

{create_meals_table_command}

{create_mealentries_table_command}

{create_cardioexercises_table_command}

{create_strengthexercises_table_command}

{create_measurements_table_command}
"""

create_mfp_database = SQLiteScript(
    name="Create MFP DB (if not existing)",
    db="database/mfp.db",  # TODO: Improve connection string passing
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
    #  Gather required parameters at runtime:
    from_date = Parameter(name="from_date", required=True)
    to_date = Parameter(name="to_date", required=True)
    username = Parameter(name="username", required=True)
    password = Parameter(name="password", required=True)

    #  Prepeare a list of dates to be scraped:
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
        username="",
        password="",
    )
