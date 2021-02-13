import datetime
import pathlib
import sqlite3
import tempfile
from contextlib import closing
from datetime import timedelta

import pytest
from prefect import Flow
from prefect.core import Parameter
from prefect.tasks.database.sqlite import SQLiteQuery

import myfitnesspaw
from myfitnesspaw import tasks


@pytest.fixture(scope="module")
def dbpath():
    with tempfile.TemporaryDirectory() as tmpdir:
        test_dbpath = pathlib.Path(tmpdir).joinpath("mfp_test.db")
        yield test_dbpath


@pytest.fixture(scope="function")
def db():
    fake_db = """
    CREATE TABLE RawDayData (userid text, date text, rawdaydata json, PRIMARY KEY(userid, date));
    CREATE TABLE Water (userid text, date text, quantity real, PRIMARY KEY(userid, date),
       CONSTRAINT fk_rawdaydata FOREIGN KEY(userid, date)
       REFERENCES RawDayData(userid, date) ON DELETE CASCADE
    );
    INSERT INTO RawDayData (userid, date, rawdaydata) VALUES
    ('fake@fakest.com', '2021-01-01', '[{}]'),
    ('fake@fakest.com', '2021-01-02', '[{}]'),
    ('fake@fakest.com', '2021-01-03', '[{}]');
    INSERT INTO Water (userid, date, quantity) VALUES
    ('fake@fakest.com', '2021-01-01', 0),
    ('fake@fakest.com', '2021-01-02', 150.0),
    ('fake@fakest.com', '2021-01-03', 2230.5);
    """

    sql_script = """
    CREATE TABLE TEST (NUMBER INTEGER, DATA TEXT);
    INSERT INTO TEST (NUMBER, DATA) VALUES
    (11, 'first'),
    (12, 'second'),
    (13, 'third');
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        test_db = pathlib.Path(tmpdir).joinpath("test.db")
        with closing(sqlite3.connect(test_db)) as conn:
            with closing(conn.cursor()) as c:
                c.executescript(sql_script)
                conn.commit()
        yield test_db


@pytest.fixture()
def myfitnesspal_day(mocker):
    day = mocker.Mock(myfitnesspaw.tasks.myfitnesspal.day.Day)
    day.date = datetime.date(2021, 1, 1)
    day.meals = []
    yield day


@pytest.fixture()
def fake_materialized_days(mocker):
    fake_username = "fake@fakest.com"
    fake_dates = [
        datetime.date(2021, 1, 1),
        datetime.date(2021, 1, 2),
        datetime.date(2021, 1, 3),
    ]
    fake_notes = [
        {"type": "food", "date": "2021-01-01", "body": "notable"},
        {"type": "food", "date": "2021-01-02", "body": ""},
        {"type": "food", "date": "2021-01-03", "body": "noted"},
    ]
    fake_water = [0, 2160.0, 1500]

    fake_days = []
    for i in range(3):
        day = mocker.Mock(myfitnesspaw._utils.MaterializedDay)
        day.username = fake_username
        day.date = fake_dates[i]
        day.notes = fake_notes[i]
        day.water = fake_water[i]
        fake_days.append(day)
    yield fake_days


class TestSQLiteExecuteMany:
    def test__init__with_no_params__initializes_task(self):
        task = tasks.SQLiteExecuteMany()
        assert task

    def test__run__when_query_not_provided_and_not_available__raises_ValueError(
        self, db
    ):
        data = [(14, "fourth"), (15, "fifth")]
        task = tasks.SQLiteExecuteMany(db=db, data=data)
        with pytest.raises(ValueError, match="A query string must be provided"):
            task.run()

    def test__run__when_data_not_provided_and_not_available__raises_ValueError(
        self, db
    ):
        query = ("INSERT INTO TEST (number, data) VALUES (?, ?);",)
        task = tasks.SQLiteExecuteMany(db=db, query=query)
        with pytest.raises(ValueError, match="A data list must be provided"):
            task.run()

    def test__initialization__with_all_params__initializes_and_runs_task(self, db):
        with Flow(name="Test") as f:
            ins = tasks.SQLiteExecuteMany(db=db)(
                query="INSERT INTO TEST (number, data) VALUES (?, ?);",
                data=[(14, "fourth"), (15, "fifth")],
            )
            sel = SQLiteQuery(db=db, query="SELECT * FROM TEST")()

        out = f.run()

        assert out.is_successful()
        result = out.result[sel].result
        assert result == [
            (11, "first"),
            (12, "second"),
            (13, "third"),
            (14, "fourth"),
            (15, "fifth"),
        ]

    def test__task_init_with_no_params__runs_when_params_passed_to_run(self, db):
        task = tasks.SQLiteExecuteMany(db=db)
        with Flow(name="Flow") as f:
            query = "INSERT INTO TEST (number, data) VALUES (?, ?);"
            data = [(14, "fourth"), (15, "fifth")]
            ins = task(query=query, data=data)
            sel = SQLiteQuery(db=db, query="SELECT * FROM TEST")()

        out = f.run()

        assert out.is_successful()
        result = out.result[sel].result
        assert result == [
            (11, "first"),
            (12, "second"),
            (13, "third"),
            (14, "fourth"),
            (15, "fifth"),
        ]


class TestETLTasks:
    def test__prepare_extraction_start_end_dates__without_dates__returns_defaults(self):
        with Flow(name="test") as f:
            from_date = Parameter("from_date", default=None)
            to_date = Parameter("to_date", default=None)
            task = tasks.prepare_extraction_start_end_dates(from_date, to_date)

        out = f.run()

        today = datetime.datetime.now().date()
        expected_from = today - timedelta(days=6)
        expected_to = today - timedelta(days=1)
        assert out.is_successful()
        assert out.result[task].result == (expected_from, expected_to)

    def test__prepare_extraction_start_end_dates__with_both_dates__returns_passed(self):
        from_date_str = "01.01.2021"
        to_date_str = "02.01.2021"
        with Flow(name="test") as f:
            from_date = Parameter("from_date", default=None)
            to_date = Parameter("to_date", default=None)
            task = tasks.prepare_extraction_start_end_dates(from_date, to_date)

        out = f.run(from_date=from_date_str, to_date=to_date_str)

        expected_from = datetime.datetime.strptime(from_date_str, "%d.%m.%Y").date()
        expected_to = datetime.datetime.strptime(to_date_str, "%d.%m.%Y").date()
        assert out.is_successful()
        assert out.result[task].result == (expected_from, expected_to)

    def test__prepare_extraction_start_end_dates__with_a_single_date__raises_ValueError(
        self,
    ):
        from_date_str = "01.01.2021"
        with Flow(name="test") as f:
            from_date = Parameter("from_date", default=None)
            to_date = Parameter("to_date", default=None)
            task = tasks.prepare_extraction_start_end_dates(from_date, to_date)

        out = f.run(from_date=from_date_str)

        assert out.is_failed()
        assert isinstance(out.result[task].result, ValueError)
        assert "Either both from_date and to_date" in str(out.result[task])

    def test__generate_dates_to_extract__with_good_dates__returns_correct_sequence(
        self,
    ):
        from_date = datetime.date(2021, 1, 1)
        to_date = datetime.date(2021, 1, 5)
        expected_result = [
            datetime.date(2021, 1, 1),
            datetime.date(2021, 1, 2),
            datetime.date(2021, 1, 3),
            datetime.date(2021, 1, 4),
            datetime.date(2021, 1, 5),
        ]

        with Flow(name="test") as f:
            task = tasks.generate_dates_to_extract(from_date, to_date)

        out = f.run()

        assert out.is_successful()
        assert out.result[task].result == expected_result

    def test__generate_dates_to_extract__with_bad_dates__raises_ValueError(
        self,
    ):
        from_date = datetime.date(2021, 1, 2)
        to_date = datetime.date(2021, 1, 1)

        with Flow(name="test") as f:
            task = tasks.generate_dates_to_extract(from_date, to_date)

        out = f.run()

        assert out.is_failed()
        assert isinstance(out.result[task].result, ValueError)
        assert "to_date cannot be before from_date" in str(out.result[task])

    def test__create_mfp_database__with_no_existing_database__creates_mfp_database(
        self, dbpath, monkeypatch
    ):
        monkeypatch.setattr(tasks, "DB_PATH", dbpath)

        with Flow(name="test") as f:
            task = tasks.create_mfp_database()

        out = f.run()

        expected_tables = [
            "RawDayData",
            "Notes",
            "Water",
            "Goals",
            "Meals",
            "MealEntries",
            "CardioExercises",
            "StrengthExercises",
            "Measurements",
        ]
        tbl_query = "SELECT name FROM sqlite_master WHERE type='table';"

        with closing(sqlite3.connect(dbpath)) as conn, closing(conn.cursor()) as c:
            c.execute(tbl_query)
            # because tuple is returned for each table name
            actual_tables = [res[0] for res in c.fetchall()]

        assert out.is_successful()
        assert all(tbl in actual_tables for tbl in expected_tables)

    def test__get_myfitnesspal_day__with_good_params__returns_materialized_day(
        self, mocker, myfitnesspal_day
    ):
        fake_myfitnesspal = mocker.patch("myfitnesspaw.tasks.myfitnesspal")
        fake_client = mocker.patch("myfitnesspaw.tasks.myfitnesspal.Client")
        fake_day = mocker.patch("myfitnesspaw.tasks.myfitnesspal.day.Day")
        fake_myfitnesspal.Client.return_value = fake_client
        fake_client.get_date.return_value = myfitnesspal_day

        fake_username = "foofoo"
        fake_password = "barbar"
        query_date = datetime.date(2021, 1, 1)
        with Flow(name="test") as f:
            task = tasks.get_myfitnesspal_day(fake_username, fake_password, query_date)

        out = f.run()

        assert out.is_successful()

    def test__extract_notes_from_days__with_days_list__returns_notes_values(
        self, fake_materialized_days
    ):
        expected_result = [
            ("fake@fakest.com", datetime.date(2021, 1, 1), "food", "notable"),
            ("fake@fakest.com", datetime.date(2021, 1, 3), "food", "noted"),
        ]
        with Flow(name="test") as f:
            task = tasks.extract_notes_from_days(fake_materialized_days)

        out = f.run()

        result = out.result[task].result
        assert out.is_successful()
        assert expected_result == result

    def test__extract_water_from_days__with_days_list__returns_water_values(
        self, fake_materialized_days
    ):
        expected_result = [
            ("fake@fakest.com", datetime.date(2021, 1, 1), 0.0),
            ("fake@fakest.com", datetime.date(2021, 1, 2), 2160.0),
            ("fake@fakest.com", datetime.date(2021, 1, 3), 1500.0),
        ]
        with Flow(name="test") as f:
            task = tasks.extract_water_from_days(fake_materialized_days)

        out = f.run()

        result = out.result[task].result
        assert out.is_successful()
        assert expected_result == result
