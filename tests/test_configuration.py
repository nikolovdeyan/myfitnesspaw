import os
from pathlib import Path

import toml

ROOT_DIR = Path().absolute()
MFP_CONFIG_FILE = "mfp_config.toml"

current_mfp_config = """
[cloud]
use_local_secrets = true

[context.secrets]
EMAIL_USERNAME = ""
EMAIL_PASSWORD = ""
MYFITNESSPAL_USERNAME_USER = ""
MYFITNESSPAL_PASSWORD_USER = ""
MYFITNESSPAW_SLACK_WEBHOOK_URL = ""
MYFITNESSPAW_DROPBOX_ACCESS_TOKEN = ""

[myfitnesspaw]
mfp_db_file = "mfp_db.sqlite"

[myfitnesspaw.backup]
dbx_backup_dir = "/mfp_db_backups"
"""


class TestMFPRepo:
    def test_configuration_file_is_available(self):
        assert os.path.exists(ROOT_DIR.joinpath(MFP_CONFIG_FILE))

    def test_configuration_file_uses_local_secrets(self):
        mfp_config = toml.load(ROOT_DIR.joinpath(MFP_CONFIG_FILE))
        assert mfp_config["cloud"]["use_local_secrets"] is True

    def test_configuration_file_does_not_contain_secrets(self):
        mfp_config = toml.load(ROOT_DIR.joinpath(MFP_CONFIG_FILE))

        # Verify no secrets in file
        secrets = mfp_config["context"]["secrets"]
        assert not secrets["EMAIL_USERNAME"]
        assert not secrets["EMAIL_PASSWORD"]
        assert not secrets["MYFITNESSPAL_USERNAME_USER"]
        assert not secrets["MYFITNESSPAL_PASSWORD_USER"]
        assert not secrets["MYFITNESSPAW_SLACK_WEBHOOK_URL"]
        assert not secrets["MYFITNESSPAW_DROPBOX_ACCESS_TOKEN"]

    def test_configuration_file_has_default_database_file_name(self):
        mfp_config = toml.load(ROOT_DIR.joinpath(MFP_CONFIG_FILE))
        assert mfp_config["myfitnesspaw"]["mfp_db_file"] == "mfp_db.sqlite"
