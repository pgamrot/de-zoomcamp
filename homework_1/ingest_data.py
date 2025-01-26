import pandas as pd
from sqlalchemy import create_engine, text
import argparse
import subprocess
import os

# windows local testing
def download_file(url, filename):
    # --- windows version ---
    # command = f"powershell -Command Invoke-WebRequest -Uri {url} -OutFile {filename}"
    #
    # try:
    #     subprocess.run(command, check=True, shell=True)
    #     print("File downloaded successfully and saved as output.parquet")
    # except subprocess.CalledProcessError as e:
    #     print(f"Error occurred: {e}")

    # --- linux / docker version ---
    os.system(f'wget {url} -O {filename}')


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.db
    table = params.table
    url = params.url
    filetype = params.filetype

    if filetype == "parquet":
        output_name = url.split('/')[-1]
        download_file(url, output_name)
        df = pd.read_parquet(output_name)
    elif filetype == "csv":
        output_name = url.split('/')[-1]
        download_file(url, output_name)
        df = pd.read_csv(output_name)
    else:
        raise ValueError("Unsupported file type")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    df.head(n=0).to_sql(table, con=engine, if_exists='replace',index=False)
    df.to_sql(table, con=engine, if_exists='append', chunksize=100000,index=False)
    with engine.connect() as connection:
        connection.execute(text(f'ALTER TABLE {table} ADD COLUMN id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY;'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='Taxi data ingestion',
        description='Ingest parquet data to PostgreSQL',
        epilog='Created for Data Engineering Zoomcamp 2025 by PG')
    parser.add_argument('--user', help='User name for PostgreSQL')
    parser.add_argument('--password', help='Password for PostgreSQL')
    parser.add_argument('--host', help='Host for PostgreSQL')
    parser.add_argument('--port', help='Port for PostgreSQL')
    parser.add_argument('--db', help='Database to which we write data')
    parser.add_argument('--table', help='Table name in which we write data')
    parser.add_argument('--url', help='Url containing data')
    parser.add_argument('--filetype', help='Filetype of the file containing data')

    args = parser.parse_args()

    main(args)
