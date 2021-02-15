# Postresql TPC-H benchmark

The instructions are heavily based on this [guide](https://ankane.org/tpc-h) 
1. You need to have postgresql installed
1. Generate required scripts
    ```
    git clone https://github.com/gregrahn/tpch-kit.git
    cd tpch-kit/dbgen
    make
    ```
1. Create empty database for benchmark
    ```
       createdb <DATABASE_NAME>
       psql <DATABASE_NAME> -f dss.ddl
    ```
1. Generate tpc-h data csvs
    ```
    ./dbgen -vf -s <SCALE FACTOR>
    ```
1. Load csvs to database tables 
    ```
    for i in `ls *.tbl`; do
      table=${i/.tbl/}
      echo "Loading $table..."
      psql <DATABASE_NAME> -q -c "TRUNCATE $table"
      psql <DATABASE_NAME> -c "\\copy $table FROM '/tmp/$i' CSV DELIMITER '|'"
    done
    ```

1. Generate Queries
    ```
    DSS_QUERY=/tmp/queries ./qgen | sed 's/limit -1//' | sed 's/day (3)/day/
    ```

1. Run analyze
    ```
    psql tpch -c "ANALYZE VERBOSE"
    ```

1. [Create indexes](https://github.com/dimitri/tpch-citus/blob/master/schema/tpch-index.sql)

1. Run benchmark_tpch.py on all the database you created (different scales) 
    ```
    python benchmark_tpch.py <CONN_STR>
    ```