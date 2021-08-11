import pytest

from myfitnesspaw import flows


class TestFlows:
    def test__get_etl_flow__without_passed_user__raises_ValueError(self):
        with pytest.raises(ValueError, match="An user must be provided for the flow"):
            flows.get_etl_flow(username=None)

    def test__get_etl_flow__without_passed_flow_name__applies_default_name(self):
        username = "testuser"

        f = flows.get_etl_flow(username=username)

        assert username.upper() in f.name

    def test__get_etl_flow__with_passed_flow_name__applies_passed_name(self):
        username = "testuser"
        flow_name = "test_flow"

        f = flows.get_etl_flow(username=username, flow_name=flow_name)

        assert flow_name in f.name

    def test__get_progress_report_flow__without_passed_user__raises_ValueError(self):
        with pytest.raises(ValueError, match="An user must be provided for the flow"):
            flows.get_progress_report_flow(username=None)

    def test__get_report_flow__without_passed_flow_name__applies_default_name(self):
        username = "testuser"

        f = flows.get_progress_report_flow(username=username)

        assert username.upper() in f.name

    def test__get_report_flow__with_passed_flow_name__applies_passed_name(self):
        username = "testuser"
        flow_name = "test_flow"

        f = flows.get_progress_report_flow(username=username, flow_name=flow_name)

        assert flow_name in f.name

    def test__get_backup_flow__without_passed_flow_name__applies_default_name(self):
        f = flows.get_backup_flow()

        assert "MyFitnessPaw DB Backup" in f.name

    def test__get_backup_flow__with_passed_flow_name__applies_passed_name(self):
        flow_name = "test_flow"

        f = flows.get_backup_flow(flow_name=flow_name)

        assert flow_name in f.name
