
from sqlalchemy import create_engine
from tqdm import tqdm


def load_tpch(conn_str):
    with create_engine(conn_str).connect() as con:
        for cmd_file in tqdm(["tpch-load.sql", "tpch-pkeys.sql", "tpch-alter.sql",  "tpch-index.sql"]):
            print(f"Executing {cmd_file}")
            with open(f'datagen/tpch_assets/dss/{cmd_file}') as query_file:
                con.execute(query_file.read())


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Create TPC-H dataset.')
    parser.add_argument('conn_str', type=str, help='SQLAlchemy connection string.')
    args = parser.parse_args()
    load_tpch(args.conn_str)
