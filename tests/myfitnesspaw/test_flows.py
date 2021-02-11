import pytest

from myfitnesspaw import flows


class TestFlows:
    def test__get_etl_flow__without_passed_user__raises_ValueError(self):
        with pytest.raises(ValueError, match="An user must be provided for the flow."):
            flows.get_etl_flow(user=None)

    def test__get_etl_flow__without_passed_name__applies_default_name(self):
        user = "testuser"

        f = flows.get_etl_flow(user=user)

        assert user.upper() in f.name

    def test__get_etl_flow__with_passed_name__applies_passed_name(self):
        user = "testuser"
        flow_name = "test_flow"

        f = flows.get_etl_flow(user=user, flow_name=flow_name)

        assert flow_name in f.name
