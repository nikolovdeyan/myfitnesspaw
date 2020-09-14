import datetime
import sqlite3
from contextlib import closing
from datetime import timedelta

import jsonpickle
import myfitnesspal
import prefect
from prefect import Flow, Parameter, task, unmapped
from prefect.engine.state import Failed
from prefect.tasks.database.sqlite import SQLiteScript
from prefect.utilities.notifications import slack_notifier

handler = slack_notifier(only_states=[Failed])  # we can call it early

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
    db="mfp.db",
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


@task(name="Get day record json from date (mfp database)")
def get_db_record_day_serialized(username, date):
    sql_stmt = """
    SELECT rawdaydata FROM RawDayData
    WHERE userid=? AND date=?
    """
    with closing(sqlite3.connect("mfp.db")) as conn, closing(conn.cursor()) as cursor:
        cursor.execute(sql_stmt, (username, date))
        result = cursor.fetchone()
        day_json = result[0] if result else None
    return (username, date, day_json)


@task(
    name="Get Myfitnesspal Measure records",
    timeout=5,
    max_retries=10,
    retry_delay=timedelta(seconds=10),
)
def get_myfitnesspal_measure_records(username, password, from_date, to_date, measure):
    client = myfitnesspal.Client(username=username, password=password)
    try:
        measure_response = client.get_measurements(measure, from_date, to_date)
    except ValueError as e:
        if str(e) == f"Measurement '{measure}' does not exist.":
            return []
        else:
            raise e
    measure_records = list(measure_response.items())  # convert to a list of tuples
    measure_records = [(username, measure) + t for t in measure_records]
    return measure_records


@task(name="Extract meals from Day")
def extract_meals_from_days_list(days_list):
    for day in days_list:
        for meal in day.meals:
            if not meal:  # TODO: ?
                continue
            else:
                meal.username = day.username
                meal.date = day.date
    meals = [meal for day in days_list for meal in day.meals if meal]
    return meals


@task(name="Extract mealentries from meals")
def extract_mealentries_from_meals_list(meals_list):
    mealentries = []
    for meal in meals_list:
        for entry in meal.entries:
            meal_entry = (
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
            mealentries.append(meal_entry)
    return mealentries


@task(name="Extract cardio exercises from Day")
def extract_cardio_exercises_from_day(day_data):
    exercises_list = []
    for record in day_data.exercises[0]:
        exercise_entry = (
            day_data.username,
            day_data.date,
            record.name,
            record.nutrition_information.get("minutes", None),
            record.nutrition_information.get("calories burned", None),
        )
        exercises_list.append(exercise_entry)
    return exercises_list


@task(name="Extract strength exercises from Day")
def extract_strength_exercises_from_day(day_data):
    exercises_list = []
    for record in day_data.exercises[1]:
        exercise_entry = (
            day_data.username,
            day_data.date,
            record.name,
            record.nutrition_information.get("sets", None),
            record.nutrition_information.get("reps/set", None),
            record.nutrition_information.get("weight/set", None),
        )
        exercises_list.append(exercise_entry)
    return exercises_list


@task(name="Filter records to insert or replace")
def filter_records_to_upsert(myfitnesspal_data, local_data):
    logger = prefect.context.get("logger")
    records_to_upsert = [t for t in myfitnesspal_data if t not in local_data]
    logger.info(f"Records to Insert/Update: {len(records_to_upsert)}")
    return records_to_upsert


@task(name="Prepare deserialized days list")
def deserialize_records_to_process(days_tuples_list):
    result = [jsonpickle.decode(day_json[2]) for day_json in days_tuples_list]
    return result


@task(name="MFP Load StrengthExercises in database.")
def mfp_insert_strength_exercises(strength_list):
    insert_strengthexercises_command = """
    INSERT INTO StrengthExercises(userid, date, exercise_name, sets, reps, weight)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    with closing(sqlite3.connect("mfp.db")) as conn, closing(conn.cursor()) as cursor:
        cursor.executemany(insert_strengthexercises_command, strength_list)
        conn.commit()


@task(name="MFP Load CardioExercises in database.")
def mfp_insert_cardio_exercises(cardio_list):
    insert_cardioexercises_command = """
    INSERT INTO CardioExercises(userid, date, exercise_name, minutes, calories_burned)
    VALUES (?, ?, ?, ?, ?)
    """
    with closing(sqlite3.connect("mfp.db")) as conn, closing(conn.cursor()) as cursor:
        cursor.executemany(insert_cardioexercises_command, cardio_list)
        conn.commit()


@task(name="MFP Insert Daily Meals")
def mfp_insert_meals_list(meals_list):
    values = [
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
        for meal in meals_list
    ]
    insert_meal_command = """
    INSERT INTO Meals(userid, date, name, calories, carbs, fat, protein, sodium, sugar)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with closing(sqlite3.connect("mfp.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(insert_meal_command, values)
            conn.commit()


@task(name="MFP Insert Daily MealEntries")
def mfp_insert_mealentries_list(mealentries_list):
    insert_mealentry_command = """
    INSERT INTO MealEntries(userid, date, meal_name, short_name,
    quantity, unit, calories, carbs, fat, protein, sodium, sugar)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with closing(sqlite3.connect("mfp.db")) as conn, closing(conn.cursor()) as cursor:
        cursor.executemany(insert_mealentry_command, mealentries_list)
        conn.commit()


@task(name="MFP insert/replace raw days records")
def mfp_insert_raw_day_records(days_data_json):
    insert_raw_data_command = """
    INSERT OR REPLACE INTO RawDayData(userid, date, rawdaydata)
    VALUES (?, ?, ?)
    """
    with closing(sqlite3.connect("mfp.db")) as conn, closing(conn.cursor()) as cursor:
        cursor.executemany(insert_raw_data_command, days_data_json)
        conn.commit()


@task(name="MFP Load measurements in database")
def mfp_insert_measurements(measurements):
    insert_measurements_command = """
    INSERT INTO Measurements(userid, measure_name, date, value)
    VALUES (?, ?, ?, ?)
    """
    with closing(sqlite3.connect("mfp.db")) as conn, closing(conn.cursor()) as cursor:
        cursor.executemany(insert_measurements_command, measurements)
        conn.commit()


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

    #  Get the serialized day records already existing in the databse for
    #  the time period:
    #  TODO: Rewrite this without the map -> query the database with a single action for
    #        the complete dates list.
    db_raw_days_data = get_db_record_day_serialized.map(
        date=dates_to_scrape, username=unmapped(username)
    )

    #  Compare the existing records with the ones just scraped:
    #  Check also FilterTask:
    #  https://docs.prefect.io/api/latest/tasks/control_flow.html#filtertask
    #    true_branch = ActionIfTrue()
    #    false_branch = ActionIfFalse()
    #    ifelse(CheckCondition(), true_branch, false_branch)
    #    merged_result = merge(true_branch, false_branch)
    days_to_upsert = filter_records_to_upsert(mfp_raw_days_data, db_raw_days_data)

    #  Insert the raw data dump as a serialized json in the databse:
    r1 = mfp_insert_raw_day_records(days_to_upsert)

    #  The new/changed days_to_usert can now be deserialized and used to populate
    #  the report tables:
    days_list = deserialize_records_to_process(days_to_upsert, upstream_tasks=[r1])

    #  Prepare a list of all meals in the period from the days_list:
    meals_list = extract_meals_from_days_list(days_list)

    #  Insert the meals records in the database:
    r2 = mfp_insert_meals_list(meals_list)

    #  Extract individual meal entries from each meal from the list:
    mealentries_list = extract_mealentries_from_meals_list(meals_list)

    #  Insert the meal entries records in the database:
    r3 = mfp_insert_mealentries_list(mealentries_list)

#    cardio_exercise_entries = extract_cardio_exercises_from_day.map(day_data=days_data)
#
#    #  Inserth the exercise entries gathered for cardio:
#    t3 = mfp_insert_cardio_exercises.map(
#        cardio_list=cardio_exercise_entries, upstream_tasks=[t1])
#
#    strength_exercise_entries = extract_strength_exercises_from_day.map(
#          day_data=days_data)
#
#    #  Inserth the exercise entries gathered for strength exercises:
#    t4 = mfp_insert_strength_exercises.map(strength_list=strength_exercise_entries,
#             upstream_tasks=[t1])
#
#    measurements_list = get_myfitnesspal_measure_records.map(
#        measure=['Weight', 'Height'],
#        username=unmapped(username),
#        password=unmapped(password),
#        from_date=unmapped(from_date),
#        to_date=unmapped(to_date),
#        upstream_tasks=[t1],
#    )
#
#    #  Insert gathered measurements
#    t5 = mfp_insert_measurements.map(measurements_list)


# flow.visualize()
# flow.register(project_name="MFP Prototype")

if __name__ == "__main__":
    flow.run(
        from_date=datetime.date(2020, 9, 8),
        to_date=datetime.date(2020, 9, 10),
        username="",
        password="",
    )
