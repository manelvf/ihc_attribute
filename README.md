# IHC Attribution

## Description

A data processing service that analyzes customer journey data to determine marketing channel attribution for conversions. It processes customer interactions in batches through the IHC Attribution API to calculate incremental holding contribution (IHC) values for each touchpoint. The service generates detailed reports with key marketing metrics like Cost Per Order (CPO) and Return on Ad Spend (ROAS) to help optimize marketing spend across channels.

Key features:
- Batch processing of customer journey data
- Integration with IHC Attribution API
- Channel-level attribution reporting
- Marketing performance metrics calculation
- Data persistence using SQLite


## Installation and Set Up

### Dependencies

Installed with Poetry (see below).

- dotenv
- typer
- requests


### Installation

Using the command line:

- Install the dependencies

```
    poetry install
```

- 

Note: A database with the history conversions should be provided.

## Execution

- Initialize the shell (this will enable the virtual environment):

```
    poetry shell 
```

- Run the service

```
   python3 main.py
```

## Output

Once the data is processed by the API, CSV files are generated in the `data` folder.

## Architecture

### Components

The `main.py` modules calls the components other components that are in the `src` folder. All components in that folder call the DB module, but none of them call each other, in order to keep the modules decoupled.

Those are the components and their purpose:
- `batch_processor.py`: Handles processing customer journeys in batches, sending them to the IHC Attribution API, and storing the responses. It manages batch sizes and error handling for API requests.

- `db.py`: Provides database operations including creating tables, inserting data, and querying results. Contains functions for managing customer journeys, session costs, and channel reporting data in SQLite.

- `ihc_attribution_client.py`: Client for interacting with the IHC Attribution API. Handles API authentication, request formatting, and response parsing.

- `report.py`: Generates reports and metrics from the processed attribution data. Calculates key metrics like CPO (Cost Per Order) and ROAS (Return on Ad Spend) and outputs them to CSV files.

### Pipeline Workflow

The execution workflow is the following:

- Initialize DB.
- Read database elements.
- Fill database table.
- Call IHC API.
- Process the responses and store them in db.
- Generate report based on the processed answers.

Most of the workload is performed by the `batch_processor`  module, with a final call to the `report` module.


## TODO

- Make use of object orientation for keeping track of the DB connection.
- Make the DB module generic so the database could be replaced by a different one when required.
- Add unit tests and e2e tests.
- Execute the tests in a CI/CD pipeline.
- Add benchmarks for identifying the bottlenecks.
