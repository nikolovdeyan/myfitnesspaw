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

select_alpha_report_totals = """
WITH params(username) AS  (SELECT ?)
  SELECT 'days_total', COUNT(*) FROM params, RAWDAYDATA RDD WHERE RDD.USERID = params.username
  UNION ALL
  SELECT 'days_with_meals' , COUNT(*) FROM params, MEALS M   WHERE M.USERID  = params.username
  UNION ALL
  SELECT 'days_with_cardio', COUNT(*) FROM params, CARDIOEXERCISES CE WHERE CE.USERID  = params.username
  UNION ALL
  SELECT 'days_with_strength', COUNT(*) FROM params, STRENGTHEXERCISES SE WHERE SE.USERID  = params.username
  UNION ALL
  SELECT 'days_with_measures', COUNT(*) FROM params, MEASUREMENTS M2 WHERE M2.USERID  = params.username
  UNION ALL
  SELECT 'num_entries_meals', COUNT(*) FROM params, MEALS M3  WHERE M3.USERID  = params.username
  UNION ALL
  SELECT 'num_entries_cardio', COUNT(*) FROM params, CARDIOEXERCISES CE2 WHERE CE2.USERID  = params.username
  UNION ALL
  SELECT 'num_entries_strength', COUNT(*) FROM params, STRENGTHEXERCISES SE WHERE SE.USERID  = params.username
  UNION ALL
  SELECT 'num_entries_measures', COUNT(*) FROM params, MEASUREMENTS M4  WHERE M4.USERID = params.username
  UNION ALL
  SELECT 'total_calories_consumed',COALESCE (SUM(m.CALORIES), 0) FROM params, MEALS M  WHERE M.USERID  = params.username
  UNION ALL
  SELECT 'total_calories_exercised', COALESCE (SUM(CE.CALORIES_BURNED), 0) FROM params, CARDIOEXERCISES CE WHERE CE.USERID  = params.username
"""

select_alpha_report_range = """
WITH params(username, date_from, date_to) AS  (SELECT ?, ?, ?)
  SELECT 'days_total',COUNT(*) FROM params, RAWDAYDATA RDD WHERE RDD.USERID = params.username AND RDD.DATE  BETWEEN PARAMS.date_from and PARAMS.date_to
  UNION ALL
  SELECT 'meals_consumed', COUNT(*) from params, MEALS M WHERE M.USERID  = params.username AND M.DATE BETWEEN PARAMS.date_from and PARAMS.date_to
  UNION ALL
  SELECT 'meals_breakfast', COUNT(*) from params, MEALS M WHERE M.USERID = params.username AND M.NAME = 'breakfast' AND M.DATE BETWEEN PARAMS.date_from and PARAMS.date_to
  UNION ALL
  SELECT 'meals_lunch', COUNT(*) from params, MEALS M WHERE M.USERID = params.username AND M.NAME = 'lunch' AND M.DATE BETWEEN PARAMS.date_from and PARAMS.date_to
  UNION ALL
  SELECT 'meals_dinner', COUNT(*) from params, MEALS M WHERE M.USERID = params.username AND M.NAME = 'dinner' AND M.DATE BETWEEN PARAMS.date_from and PARAMS.date_to
  UNION ALL
  SELECT 'meals_snacks', COUNT(*) from params, MEALS M WHERE M.USERID = params.username AND M.NAME = 'snacks' AND M.DATE BETWEEN PARAMS.date_from and PARAMS.date_to
  UNION ALL
  SELECT 'calories_consumed', SUM(M.CALORIES) from params, MEALS M WHERE M.USERID  = params.username AND M.DATE BETWEEN PARAMS.date_from and PARAMS.date_to
  UNION ALL
  SELECT 'calories_breakfast',  SUM(M.CALORIES) from params, MEALS M WHERE M.USERID = params.username AND M.NAME = 'breakfast' AND M.DATE BETWEEN PARAMS.date_from and PARAMS.date_to
  UNION ALL
  SELECT 'calories_lunch', SUM(M.CALORIES) from params, MEALS M WHERE M.USERID = params.username AND M.NAME = 'lunch' AND M.DATE BETWEEN PARAMS.date_from and PARAMS.date_to
  UNION ALL
  SELECT 'calories_dinner',  SUM(M.CALORIES) from params, MEALS M WHERE M.USERID = params.username AND M.NAME = 'dinner' AND M.DATE BETWEEN PARAMS.date_from and PARAMS.date_to
  UNION ALL
  SELECT 'calories_snacks',  SUM(M.CALORIES) from params, MEALS M WHERE M.USERID = params.username AND M.NAME = 'snacks' AND M.DATE BETWEEN PARAMS.date_from and PARAMS.date_to
"""
