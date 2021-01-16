# Postresql TPC-H benchmark
1) You need to have postgresql installed
1) Follow the [installation guide for postgresql](https://ankane.org/tpc-h)
1) [Create indexes](https://github.com/dimitri/tpch-citus/blob/master/schema/tpch-index.sql)
1) Run benchmark_tpch.py to understand the queries/db bottlenecks 
1) Optimize the queries and db settings
1) Run benchmark_tpch.py to see the improvements

CREATE INDEX IDX_LINEITEM_SHIPDATE ON LINEITEM (L_SHIPDATE, L_DISCOUNT, L_QUANTITY);
CREATE INDEX IDX_ORDERS_ORDERDATE ON ORDERS (O_ORDERDATE);