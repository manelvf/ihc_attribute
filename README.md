# IHC Attribution

## Description

A data processing service that analyzes customer journey data to determine marketing channel attribution for conversions. It processes customer interactions in batches through the IHC Attribution API to calculate incremental holding contribution (IHC) values for each touchpoint. The service generates detailed reports with key marketing metrics like Cost Per Order (CPO) and Return on Ad Spend (ROAS) to help optimize marketing spend across channels.

Uses Apache Airflow for scheduling the tasks and executes them sequentially using a pipeline.

Key features:
- Batch processing of customer journey data
- Integration with IHC Attribution API
- Channel-level attribution reporting
- Marketing performance metrics calculation
- Data persistence using SQLite


## Development notes

First I started to develop the service as a monolith, then I split it in different independent modules, and for the last part I added the task framework in order to schedule the execution.

Data is stored in an *sqlite* database. The schema is provided in the fixtures folder.

Modules are included in the `lib` folder inside the `dags` folder, but they could be extracted to a separated one, as long as their path is reachable. 


## Installation and Set Up

### Dependencies

Installed with Poetry (see below).

- Apache Airflow
- dotenv
- requests
- apache-airflow


### Installation

Using the command line:

- Install the dependencies

```
    poetry install
```

- Initialize the poetry shell:

```
    poetry shell
```

- Setup Apache Airflow (https://airflow.apache.org/docs/apache-airflow/stable/start.html).

    Airflow DAGs folder configuration should point to the dags folder.


- Add root folder to the python path:

```
    export PYTHONPATH=$PYTHONPATH:<root folder>
```

- Execute setup.py for initializing the database:

```
    python setup.py
```

Note: A database with the history conversions should be provided.

## Execution

- Initialize the virtual environment (in case it isn't):

```
    poetry shell 
```

- Start the Airflow server.

```
    airflow standalone
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

For the development, I followed the path of retrieving the data that I needed from the tables, then execute the tranformations needed for calling the API, and then accumlate the results that the API sent.

The last part was the creation of the CSV reports, once the data was already inserted in the result tables.

The execution workflow is the following:

- Initialize DB. Clean result tables if needed.
- Read database elements.
- Fill database tables.
- Call IHC API.
- Process the responses calculating  and store them in db.
- Generate report based on the processed answers.

Most of the workload is performed by the `batch_processor`  module, with a final call to the `report` module.


## TODO

- Make use of object oriented classes for keeping track of the DB connection.
- Make the DB module generic so the database could be replaced by a different one when required.
- Dockerize the application for deployment. Use gunicorn or equivalent for serving the application.
- Make it a web service if required.
- Sanitize the inputs properly.
- Add authentication and authorization if required, in order to enforce the security.
- Retry API call when they fail.
- Add unit tests and e2e tests.
- Execute the tests in a CI/CD pipeline.
- Add benchmarks for identifying the bottlenecks.
