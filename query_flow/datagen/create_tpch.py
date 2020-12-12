
from sqlalchemy import create_engine
from tqdm import tqdm


def load_tpch():
    with create_engine('postgresql:///etrabelsi_thesis').connect() as con:
        for cmd_file in tqdm(["tpch-load.sql", "tpch-pkeys.sql", "tpch-alter.sql",  "tpch-index.sql"]):
            print(f"Executing {cmd_file}")
            with open(f'datagen/tpch_assets/dss/{cmd_file}') as query_file:
                con.execute(query_file.read())


if __name__ == "__main__":
    load_tpch()
