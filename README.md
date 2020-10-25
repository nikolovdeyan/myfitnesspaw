<!--
this file uses the README template found here:
https://github.com/othneildrew/Best-README-Template/blob/master/BLANK_README.md
-->

<!-- PROJECT LOGO -->
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

To get a local copy up and running follow these simple steps.

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
3. Setup secrets:
3.1 Copy secrets template
```sh
cp resources/set_env.template.sh secrets/set_environment.sh
```
3.2 Fill in the required secrets in the file.


## Usage

### Core only, in process
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
python -c 'import myfitnesspaw as mfp; mfp.etl.run(from_date="2020/10/01", to_date="2020/10/02")'
```

## Development
### Development Installation


## License
