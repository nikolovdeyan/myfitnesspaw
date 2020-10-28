<!--
this file uses the README template found here:
https://github.com/othneildrew/Best-README-Template/blob/master/BLANK_README.md
-->

<br />
<p align="center">
  <a href="https://github.com/nikolovdeyan/myfitnesspaw">
    <img src="resources/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">My Fitness Paw</h3>

  <p align="center">
    Myfitnesspal user data harvesting and reporting pipeline built with Prefect.
    <br />
  </p>
</p>


## Table of Contents

* [Introduction](#introduction)
  * [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)
  * [Running locally without orchestration](#running-locally-without-orchestration)
  * [Running with Prefect Cloud](#running-with-prefect-cloud)
  * [Setup Slack notifications](#setup-slack-notifications)
* [License](#license)


## Introduction

MyFitnessPaw utilizes the [prefect](https://github.com/PrefectHQ/prefect) data pipeline orchestration tool and the [python-myfitnesspal](https://github.com/coddingtonbear/python-myfitnesspal) library to take control of your Myfitnesspal data logs. It loads your exercise and eating habits data records to a local SQLite database and prepares convenient reporting tables. To accomplish this MyFitnessPaw provides several parameterized workflows that could be scheduled and ran directly or registered with Prefect Cloud/Prefect Server and orchestrated from there.

### Built With

* [myfitnesspal](https://github.com/coddingtonbear/python-myfitnesspal)
* [Prefect]()


## Getting Started
### Prerequisites

* Python 3.8
* pipenv (`pip install --user pipenv`)

### Installation

1. Get the project:
```sh
git clone https://github.com/nikolovdeyan/myfitnesspaw.git
```
2. Install dependencies in a virtual environment:
```sh
pipenv install
```
3. Copy secrets template
```sh
cp resources/set_env.template.sh secrets/set_environment.sh
```
4. Fill in the required secrets in `secrets/set_environment.sh`


## Usage
### Running locally without orchestration
1. In the project directory run a shell in the virtual environment: 
```sh
pipenv shell
```
2. Ensure Prefect backend is running in server mode:
```sh
prefect backend server
```
3. Source the secrets env variables: 
```sh
source secrets/set_environment.sh
```
4. Run etl: 
```sh
python -c 'import myfitnesspaw as mfp; mfp.etl.flow.run(from_date="2020/10/01", to_date="2020/10/02")'
```

### Running with Prefect Cloud
1. In the project directory run a shell in the virtual environment and set the Prefect backend to cloud mode:
```sh
pipenv shell
prefect backend cloud
```
2. Create a personal access token on https://cloud.prefect.io (User -> Personal Access Tokens -> create a new USER token)
3. Authenticate with the backend with the access token:
```sh
prefect auth login -t <user token>
```
4. Create an agent authentication token (RUNNER scoped API token. Add to configuration `PREFECT__CLOUD__AGENT__AUTH_TOKEN`)
5. Source the configuration variables and run the Prefect agent:
```sh
source secrets/set_environment.sh
prefect agent start -t $PREFECT__CLOUD__AGENT__AUTH_TOKEN -n agent-lisko &
```
6. Confirm the agent is visible in https://cloud.prefect.io.
7. Create a project in https://cloud.prefect.io to associate our flows with.
8. Register flows with the project name that was just created:
```sh
python -c 'import myfitnesspaw as mfp; mfp.etl.flow.register(project_name="Project Name")'
```
9. The flow can now be scheduled and run from the Prefect Cloud UI. 


### Setup Slack notificatons
Myfitnesspaw provides a state handler that (if configured) will send a notification to the slack channel on each flow failure. More information is available in [Prefect documentation](https://docs.prefect.io/core/advanced_tutorials/slack-notifications.html#installation-instructions). To enable follow the steps below:

1. Go to the "installation URL" link available on the Prefect documentation page above and select the channel where the notifications will appear. 
2. Click on "Authorize" to generate the unique webhook url. Copy the url string and paste it in the `secrets/set_environment.sh` file as the `MYFITNESSPAW_SLACK_WEBHOOK_URL` environmental variable.
3. Run `source secrets/set_environment.sh` to apply the new variable before the next flow run. You should now receive notifications on flow run failures in the selected Slack channel.

## License
[MIT](https://opensource.org/licenses/MIT)
