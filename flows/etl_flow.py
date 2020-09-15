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


@task(
    name="Get Measure Records for Dates <- (myfitnesspal)",
    timeout=5,
    max_retries=10,
    retry_delay=timedelta(seconds=10),
)
def get_myfitnesspal_measure_records(username, password, from_date, to_date, measure):
    client = myfitnesspal.Client(username=username, password=password)
    # TODO: Fix this logic
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


@task(name="Serialize Day Records List")
def serialize_myfitnesspal_days(myfitnesspal_days):
    """Prepare a list of Day records for database load."""
    days_values = [
        (day._username, day._date, jsonpickle.encode(day)) for day in myfitnesspal_days
    ]
    return days_values


@task(name="Filter New or Changed Day Records")
def filter_new_or_changed_records(extracted_records, local_records):
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


@task(name="Deserialize Day Records to Process")
def deserialize_records_to_process(serialized_days):
    result = [jsonpickle.decode(day_json[2]) for day_json in serialized_days]
    return result


@task(name="Extract Meals from Day Sequence")
def extract_meals_from_days(days):
    for day in days:
        for meal in day.meals:
            if not meal:  # TODO: ?
                continue
            else:
                meal.username = day._username
                meal.date = day._date
    meals = [meal for day in days for meal in day.meals if meal]
    return meals


@task(name="Extract Meal Records from Meal Sequence")
def extract_meal_records_from_meals(meals):
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
def extract_mealentry_records_from_meals(meals):
    mealentries = []
    for meal in meals:
        for entry in meal.entries:
            mealentry = (
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
            mealentries.append(mealentry)
    return mealentries


@task(name="Extract Cardio Exercises from Day Sequence")
def extract_cardio_exercises_from_days(days):
    cardio_list = []
    for day in days:
        for record in day._exercises[0]:
            exercise_entry = (
                day._username,
                day._date,
                record.name,
                record.nutrition_information.get("minutes", None),
                record.nutrition_information.get("calories burned", None),
            )
        cardio_list.append(exercise_entry)
    return cardio_list


@task(name="Extract Strength Exercises from Days Sequence")
def extract_strength_exercises_from_days(days):
    strength_list = []
    for day in days:
        for record in day._exercises[1]:
            exercise_entry = (
                day._username,
                day._date,
                record.name,
                record.nutrition_information.get("sets", None),
                record.nutrition_information.get("reps/set", None),
                record.nutrition_information.get("weight/set", None),
            )
            strength_list.append(exercise_entry)
    return strength_list


@task(name="Load Meal Records -> (MyFitnessPaw)")
def mfp_insert_meals(meals_values):
    with closing(sqlite3.connect("database/mfp_db.sqlite")) as conn, closing(
        conn.cursor()
    ) as cursor:
        cursor.executemany(sql.insert_meal_record, meals_values)
        conn.commit()


@task(name="Load MealEntry Records -> (MyFitnessPaw)")
def mfp_insert_mealentries(mealentries_values):
    with closing(sqlite3.connect("database/mfp_db.sqlite")) as conn, closing(
        conn.cursor()
    ) as cursor:
        cursor.executemany(sql.insert_mealentry_record, mealentries_values)
        conn.commit()


@task(name="Load CardioExercises Records -> (MyFitnessPaw)")
def mfp_insert_cardio_exercises(cardio_list):
    with closing(sqlite3.connect("database/mfp_db.sqlite")) as conn, closing(
        conn.cursor()
    ) as cursor:
        cursor.executemany(sql.insert_cardioexercises_command, cardio_list)
        conn.commit()


@task(name="Load StrengthExercises Records -> (MyFitnessPaw)")
def mfp_insert_strength_exercises(strength_list):
    with closing(sqlite3.connect("database/mfp_db.sqlite")) as conn, closing(
        conn.cursor()
    ) as cursor:
        cursor.executemany(sql.insert_strengthexercises_command, strength_list)
        conn.commit()


@task(name="Load Measurement Records -> (MyFitnessPaw)")
def mfp_insert_measurements(measurements):
    with closing(sqlite3.connect("database/mfp_db.sqlite")) as conn, closing(
        conn.cursor()
    ) as cursor:
        cursor.executemany(sql.insert_measurements_command, measurements)
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
    serialized_days_to_process = filter_new_or_changed_records(
        extracted_records=serialized_extracted_days,
        local_records=mfp_existing_days,
    )

    #  Load the transformed sequence of raw myfitnesspal days to mfp database:
    raw_days_load_state = mfp_insert_raw_days(serialized_days_to_process)

    #  The sequence of filtered records to process will be deserialized before
    #  populating the reporting table to make extracting the information easier.
    days_to_process = deserialize_records_to_process(
        serialized_days=serialized_days_to_process, upstream_tasks=[raw_days_load_state]
    )

    #  Prepare a sequence of all meals in the records to process:
    meals_to_process = extract_meals_from_days(days_to_process)

    #  Extract the meals' records to prepare for load in the database:
    meals_records = extract_meal_records_from_meals(meals_to_process)

    #  Extract individual meal entries from each meal from the list:
    mealentries_records = extract_mealentry_records_from_meals(meals_to_process)

    #  Load meals and mealentries into their respective tables:
    meals_load_state = mfp_insert_meals(meals_records)
    mealentries_load_state = mfp_insert_mealentries(mealentries_records)

    #  Extract exercises from the day records:
    cardio_exercises_to_process = extract_cardio_exercises_from_days(days_to_process)
    strength_exercises_to_process = extract_strength_exercises_from_days(
        days_to_process
    )

    #  Load exercises into their respective tables:
    cardio_exercises_load_state = mfp_insert_cardio_exercises(
        cardio_list=cardio_exercises_to_process,
    )
    strength_exercises_load_state = mfp_insert_strength_exercises(
        strength_list=strength_exercises_to_process,
    )

    #  Notes, Water, Goals are not implemented yet

    measurements_list = get_myfitnesspal_measure_records.map(
        measure=["Weight", "Height"],  # TODO: Parameterize
        username=unmapped(username),
        password=unmapped(password),
        from_date=unmapped(from_date),
        to_date=unmapped(to_date),
        upstream_tasks=[],  # TODO: Add
    )

    #  Insert gathered measurements
    measurements_load_state = mfp_insert_measurements(measurements_list)


if __name__ == "__main__":
    flow.visualize(filename="mfp_etl_dag", format="png")
    # flow.register(project_name="MFP Prototype")
    flow_state = flow.run(
        from_date=datetime.date(2020, 9, 10),
        to_date=datetime.date(2020, 9, 10),
    )
    # flow.visualize(
    #     flow_state=flow_state,
    #     filename="mfp_etl_flow_status",
    #     format="png"
    # )
