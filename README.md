# Spark ETL

## How to run

Start the vagrant vm

`vagrant up`

Get bash shell in vagrant vm

`vagrant ssh`

Set config script permission (you may not need to do this depending on how you execute)

`sudo chmod +x /vagrant/config.sh`

Move to /vagrant directory

`cd /vagrant/config`

Execute config

`./config.sh`

Install Pyspark

`./install_pyspark.sh`

Move to src directory

`cd /vagrant/src`

Execute Spark Application

`spark-submit --driver-class-path /vagrant/lib/postgresql-42.1.4.jar etl.py`


## Data Retrieval

Grabbed data using Python 3.5.2 using scripts in the data_retrieval directory


## Postgres configs (Scripted in config.sh)

You only need these steps if you aren't using the config.sh script to set everything up

* Default username is postgres.
* Default db is postgres

`sudo docker run --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres`

`sudo docker exec -it some-postgres bash`

`su postgres`

`psql`

```
CREATE TABLE stock_data (
    symbol text,
    date date,
    open int,
    high int,
    low int,
    close int,
    volume int,
    adj_close int,
    month int,
    year int,
    day int
);
```

```
CREATE TABLE avg_month_close (
    month int,
    average_month_close int
);
```

```
CREATE TABLE adjusted_close_count (
    month int,
    count int
);
```

