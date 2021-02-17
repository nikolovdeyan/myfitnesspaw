import datetime

import pytest

import myfitnesspaw
from myfitnesspaw._utils import MyfitnesspalClientAdapter


class TestMyFitnesspalClientAdapter:
    def test__init__with_no_username__raises_ValueError(self):
        expected_error_msg = "Username and password arguments must be provided."
        with pytest.raises(ValueError, match=expected_error_msg):
            client = MyfitnesspalClientAdapter(password="fakepassword")
            client.close()

    def test__init__with_no_password__raises_ValueError(self):
        expected_error_msg = "Username and password arguments must be provided."
        with pytest.raises(ValueError, match=expected_error_msg):
            client = MyfitnesspalClientAdapter(username="fakeuser")
            client.close()

    def test__enter__with_credentials__provides_client(self, mocker):
        fake_myfitnesspal = mocker.patch("myfitnesspaw._utils.myfitnesspal")
        fake_client = mocker.patch("myfitnesspaw._utils.myfitnesspal.Client")
        fake_myfitnesspal.Client.return_value = fake_client

        with MyfitnesspalClientAdapter("fakeuser", "fakepassword") as mfp_adapter:
            assert fake_client is mfp_adapter._client
