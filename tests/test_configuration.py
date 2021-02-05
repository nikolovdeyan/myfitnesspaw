import os
import sqlite3
import tempfile
from pathlib import Path

import prefect
import toml
from prefect import Flow

import myfitnesspaw as mfp

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

#@pytest.fixture
#def session(): # 1
#    connection = sqlite3.connect(':memory:')
#    db_session = connection.cursor()
#    yield db_session
#    connection.close()


#@pytest.fixture
#def setup_db(session): # 2
#    session.execute('''CREATE TABLE numbers
#                          (number text, existing boolean)''')
#    session.execute('INSERT INTO numbers VALUES ("+3155512345", 1)')
#    session.connection.commit()
#@pytest.fixture
#def 
#        testpath = Path(__file__).absolute().parent


class TestMFPRepo:

    def test_configuration_file_is_available(self):
        assert os.path.exists(ROOT_DIR.joinpath(MFP_CONFIG_FILE)) == 1

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


class TestETLFlow:
    def test_create_mfp_database(self):
        testdir = Path(__file__).absolute().parent
        fake_db_path = testdir.joinpath("mfp_db_fake.sqlite")
        mfp.DB_PATH = fake_db_path
        with Flow("someuser") as testflow:
            res = mfp.etl.create_mfp_database()
        testflow.run()

        assert False()
