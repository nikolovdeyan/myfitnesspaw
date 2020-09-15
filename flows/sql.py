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
