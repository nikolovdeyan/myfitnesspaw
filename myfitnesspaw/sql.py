"""
myfitnesspaw.sql

SQL code for the Myfitnesspaw flows.
"""

create_raw_day_table = """
CREATE TABLE IF NOT EXISTS RawDayData (
  userid text NOT NULL,
  date text NOT NULL,
  rawdaydata json,
  PRIMARY KEY(userid, date)
);
"""

create_meals_table = """
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

create_mealentries_table = """
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

create_goals_table = """
CREATE TABLE IF NOT EXISTS Goals (
  userid text NOT NULL,
  date text NOT NULL,
  calories INTEGER,
  carbs INTEGER,
  fat INTEGER,
  protein INTEGER,
  sodium INTEGER,
  sugar INTEGER,
  PRIMARY KEY(userid, date),
  CONSTRAINT fk_rawdaydata
    FOREIGN KEY(userid, date)
    REFERENCES RawDayData(userid, date)
    ON DELETE CASCADE
);
"""

create_cardioexercises_table = """
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

create_strengthexercises_table = """
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

create_notes_table = """
CREATE TABLE IF NOT EXISTS Notes (
  userid TEXT NOT NULL,
  date TEXT NOT NULL,
  type TEXT NOT NULL,
  body TEXT,
  PRIMARY KEY(userid, date),
  CONSTRAINT fk_rawdaydata
    FOREIGN KEY(userid, date)
    REFERENCES RawDayData(userid, date)
    ON DELETE CASCADE
);
"""

create_water_table = """
CREATE TABLE IF NOT EXISTS Water (
  userid TEXT NOT NULL,
  date TEXT NOT NULL,
  quantity REAL NOT NULL,
  PRIMARY KEY(userid, date),
  CONSTRAINT fk_rawdaydata
    FOREIGN KEY(userid, date)
    REFERENCES RawDayData(userid, date)
    ON DELETE CASCADE
);
"""

create_measurements_table = """
CREATE TABLE IF NOT EXISTS Measurements (
  userid text NOT NULL,
  date text NOT NULL,
  measure_name text NOT NULL,
  value REAL NOT NULL,
  PRIMARY KEY(userid, date, measure_name),
  CONSTRAINT fk_rawdaydata
    FOREIGN KEY(userid, date)
    REFERENCES RawDayData(userid, date)
    ON DELETE CASCADE
);
"""

select_rawdaydata_record = """
SELECT rawdaydata FROM RawDayData WHERE userid=? AND date=?
"""

insert_or_replace_rawdaydata_record = """
INSERT OR REPLACE INTO RawDayData(userid, date, rawdaydata)
VALUES (?, ?, ?)
"""

insert_notes = """
INSERT INTO Notes(userid, date, type, body) VALUES (?, ?, ?, ?)
"""

insert_water = """
INSERT INTO Water(userid, date, quantity) VALUES (?, ?, ?)
"""

insert_goals = """
INSERT INTO Goals(userid, date, calories, carbs, fat, protein, sodium, sugar)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
insert_meals = """
INSERT INTO Meals(userid, date, name, calories, carbs, fat, protein, sodium, sugar)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

insert_mealentries = """
INSERT INTO MealEntries(userid, date, meal_name, short_name, quantity, unit,
 calories, carbs, fat, protein, sodium, sugar)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

insert_cardioexercises = """
INSERT INTO CardioExercises(userid, date, exercise_name, minutes, calories_burned)
VALUES (?, ?, ?, ?, ?)
"""

insert_strengthexercises = """
INSERT INTO StrengthExercises(userid, date, exercise_name, sets, reps, weight)
VALUES (?, ?, ?, ?, ?, ?)
"""

insert_measurements = """
INSERT OR REPLACE INTO Measurements(userid, date, measure_name, value)
VALUES (?, ?, ?, ?)
"""

weekly_report_nutrition = """
WITH
    params(username) AS (SELECT ?),
    actual AS (
        SELECT userid, date, sum(calories) as calories_actual, sum(carbs) as carbs_actual, sum(fat) as fat_actual, sum(protein) as protein_actual, sum(sodium) as sodium_actual, sum(sugar) as sugar_actual
        FROM Meals, params
        WHERE userid = params.username and date BETWEEN datetime('now', '-8 days') AND datetime('now', '-1 days')
        GROUP BY date
    )
SELECT
'username', 'date', 'day of week',
'calories (actual)', 'calories (goal)',
'carbs (actual)', 'carbs (goal)',
'fat (actual)', 'fat (goal)',
'protein (actual)', 'protein (goal)',
'sodium (actual)', 'sodium (goal)',
'sugar (actual)', 'sugar (goal)'
UNION ALL
SELECT
    a.userid, a.date, strftime('%w', a.date) as day_of_week,
    a.calories_actual, g.calories as calories_goal,
    a.carbs_actual, g.carbs as carbs_goal,
    a.fat_actual, g.fat as fat_goal,
    a.protein_actual, g.protein as protein_goal,
    a.sodium_actual, g.sodium as sodium_goal,
    a.sugar_actual, g.sugar as sugar_goal
FROM actual a
JOIN Goals g ON a.userid = g.userid AND a.date = g.date;
"""
