import subprocess
import os
import shutil
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm


def load_sqlite_db(table):
    return pd.read_sql_query(f"SELECT * FROM {table}", sqlite3.connect("imdb.db"))


def cleanups(sqlite_db_path, sqlite_temp_files_path):
    os.remove(sqlite_db_path)
    shutil.rmtree(sqlite_temp_files_path)


def load_imdb(conn_str, sqlite_temp_files_path="downloads", sqlite_db_path="imdb.db"):
    with create_engine(conn_str).connect() as con:
        # Load regular tables
        subprocess.call(['imdb-sqlite'])
        for table in tqdm(['titles', 'people',  'akas', 'crew', 'episodes', 'ratings']):
            df = load_sqlite_db(table)
            df.to_sql(table, con, if_exists='replace')
        cleanups(sqlite_db_path, sqlite_temp_files_path, )

        # Load special tables
        genres = pd.DataFrame({"genres_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28],
                               "kid_safe": [True, True, True, True, True, True, True, True, False, True, True, True, True, True, True, True, True, True, True, False, True, True, True, True, True, True, True, True, True],
                               "genere_name": ['Mystery', 'News', 'Romance', 'Action', 'Music', 'Sci-Fi', 'Comedy', 'Thriller', 'Adult', 'Game-Show', 'Short', 'History', 'Drama', 'Crime', 'Musical', 'Documentary', 'Sport', 'Adventure', 'Biography', 'Horror', 'Film-Noir', 'Animation', 'Reality-TV', 'Western', 'Fantasy', 'Talk-Show', 'Family', 'War', '\\N']})
        genres.to_sql("genres", con, if_exists='replace')


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Create TPC-H dataset.')
    parser.add_argument('conn_str', type=str, help='SQLAlchemy connection string.')
    args = parser.parse_args()
    load_imdb(args.conn_str)
