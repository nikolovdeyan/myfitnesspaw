"""SQL code for the Myfitnesspaw flows."""

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
  PRIMARY KEY(userid, date, measure_name)
);
"""

select_rawdaydata_record = """
SELECT rawdaydata FROM RawDayData WHERE userid=? AND date=?
"""

insert_or_replace_rawdaydata_record = """
INSERT OR REPLACE INTO RawDayData(userid, date, rawdaydata)
VALUES (?, ?, ?)
"""

insert_note_record = """
INSERT INTO Notes(userid, date, type, body) VALUES (?, ?, ?, ?)
"""

insert_water_record = """
INSERT INTO Water(userid, date, quantity) VALUES (?, ?, ?)
"""

insert_goals_record = """
INSERT INTO Goals(userid, date, calories, carbs, fat, protein, sodium, sugar)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
insert_meal_record = """
INSERT INTO Meals(userid, date, name, calories, carbs, fat, protein, sodium, sugar)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

insert_mealentry_record = """
INSERT INTO MealEntries(userid, date, meal_name, short_name, quantity, unit,
 calories, carbs, fat, protein, sodium, sugar)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

insert_cardioexercises_command = """
INSERT INTO CardioExercises(userid, date, exercise_name, minutes, calories_burned)
VALUES (?, ?, ?, ?, ?)
"""

insert_strengthexercises_command = """
INSERT INTO StrengthExercises(userid, date, exercise_name, sets, reps, weight)
VALUES (?, ?, ?, ?, ?, ?)
"""

insert_measurements_command = """
INSERT OR REPLACE INTO Measurements(userid, date, measure_name, value)
VALUES (?, ?, ?, ?)
"""

select_alpha_report_data = """
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
