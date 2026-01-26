#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


# -----------------------
# Config
# -----------------------
DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
FILE_NAME = "green_tripdata_2025-11.parquet"


# -----------------------
# Core logic
# -----------------------
def ingest_data(engine, table_name):
    url = PREFIX + FILE_NAME

    df_iter = pd.read_csv(
        url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=100_000
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False
            )
            first = False
            print("✅ Table created")

        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False
        )

        print(f"✅ Inserted {len(df_chunk)} rows")


# -----------------------
# CLI
# -----------------------
@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='green_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):

    db_url = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(db_url)

    ingest_data(engine, target_table)


# -----------------------
# Entry point
# -----------------------
if __name__ == "__main__":
    run()
