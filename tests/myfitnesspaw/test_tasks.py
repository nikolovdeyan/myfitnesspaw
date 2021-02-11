import datetime
from datetime import timedelta

import pytest
from prefect import Flow
from prefect.core import Parameter

from myfitnesspaw import tasks


class TestETLTasks:
    def test__parse_date_parameters__without_dates__returns_defaults(self):
        with Flow(name="test") as f:
            from_date = Parameter("from_date", default=None)
            to_date = Parameter("to_date", default=None)
            task = tasks.parse_date_parameters(from_date, to_date)

        out = f.run()

        today = datetime.datetime.now().date()
        expected_from = today - timedelta(days=6)
        expected_to = today - timedelta(days=1)
        assert out.is_successful()
        assert out.result[task].result == (expected_from, expected_to)

    def test__parse_date_parameters__with_both_dates__returns_passed(self):
        from_date_str = "01.01.2021"
        to_date_str = "02.01.2021"
        with Flow(name="test") as f:
            from_date = Parameter("from_date", default=None)
            to_date = Parameter("to_date", default=None)
            task = tasks.parse_date_parameters(from_date, to_date)

        out = f.run(from_date=from_date_str, to_date=to_date_str)

        expected_from = datetime.datetime.strptime(from_date_str, "%d.%m.%Y").date()
        expected_to = datetime.datetime.strptime(to_date_str, "%d.%m.%Y").date()
        assert out.is_successful()
        assert out.result[task].result == (expected_from, expected_to)

    def test__parse_date_parameters__with_a_single_date__raises_ValueError(self):
        from_date_str = "01.01.2021"
        with Flow(name="test") as f:
            from_date = Parameter("from_date", default=None)
            to_date = Parameter("to_date", default=None)
            task = tasks.parse_date_parameters(from_date, to_date)

        out = f.run(from_date=from_date_str)

        assert out.is_failed()
        assert isinstance(out.result[task].result, ValueError)
        assert "Either both from_date and to_date" in str(out.result[task])
