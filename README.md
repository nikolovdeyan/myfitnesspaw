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

* [About the Project](#about-the-project)
  * [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)
* [License](#license)


## About The Project

MyFitnessPaw utilizes the [prefect](https://github.com/PrefectHQ/prefect) data pipeline orchestration tool and the [python-myfitnesspal](https://github.com/coddingtonbear/python-myfitnesspal) library to take control of your Myfitnesspal data logs. It loads your exercise and eating habits data records to a local SQLite database and prepares convenient reporting tables. To accomplish this MyFitnessPaw provides several parameterized workflows that could be scheduled and ran directly or registered with Prefect Cloud/Prefect Server and orchestrated from there.

### Built With

* [myfitnesspal](https://github.com/coddingtonbear/python-myfitnesspal)
* [Prefect]()


## Getting Started

### Prerequisites

* Python 3.8
* pipenv (`pip install --user pipenv`)

### Installation and Configuration

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

### Local usage without orchestration
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
### Slack notificatons
Myfitnesspaw provides a state handler that (if configured) will send a notification to the slack channel on each flow failure. More information is available in [Prefect documentation](https://docs.prefect.io/core/advanced_tutorials/slack-notifications.html#installation-instructions). To enable follow the steps below:

1. Go to the "installation URL" link available on the Prefect documentation page above and select the channel where the notifications will appear. 
2. Click on "Authorize" to generate the unique webhook url. Copy the url string and paste it in the `secrets/set_environment.sh` file as the `MYFITNESSPAW_SLACK_WEBHOOK_URL` environmental variable.
3. Run `source secrets/set_environment.sh` to apply the new variable before the next flow run. You should now receive notifications on flow run failures in the selected Slack channel.

## Development
### Development Installation

## License
