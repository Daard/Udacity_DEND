# Project: Data Warehouse

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## The project template includes four files:

- **create_table.py** is where I'have created my fact and dimension tables for the star schema in Redshift.
- **etl.py** is where I have loaded data from S3 into staging tables on Redshift and then process that data into my analytics tables on Redshift.
- **sql_queries.py** is where I have defined my SQL statements, which will be imported into the two other files above.
- **main.ipynb** is main notebook which guide you through this project.
- **README.md** is where I have provided discussion on my process and decisions for this ETL pipeline.

## Dependencies
- **poetry.toml** contains all neccessy deps.

## How to run
- Install poetry:
`curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python`
- Inside project rep create virtual env:
`poetry install`
`poetry shell`
- Run Jupyter lab:
`jupyter lab --port 9999`
- Open **main.ipynb** notebook and run code blocks.