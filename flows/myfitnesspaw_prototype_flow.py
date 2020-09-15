import datetime
import sqlite3
from contextlib import closing
from datetime import timedelta

import jsonpickle
import myfitnesspal
from prefect import Flow, task
from prefect.engine.state import Failed
from prefect.utilities.notifications import slack_notifier

handler = slack_notifier(only_states=[Failed])  # we can call it early


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
    #  The new/changed days_to_usert can now be deserialized and used to populate
    #  the report tables:
    r1 = None
    days_to_upsert = None
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
