# QueryFlow

QueryFlow, is a query visualization tool that provides insights into common problems in your SQL query.
QueryFlow visualizes the query execution using the Sankey diagram, a technique that allows one to illustrate complex processes, with a focus on a single aspect or resource that you want to highlight.
This allow to tackle the following problems:
-	Identifying missing records.
-	Identifying Ineffective operations.
-	Identifying duplications.
-	Identifying performance bottlenecks in a single query.
-	Identifying performance bottlenecks in multiple queries.



## Installing #
``` shell
pip install git+https://github.com/eyaltrabelsi/query-flow.git
```


## Publications #

**Title**: Visualizing Database Execution Plans using
Sankey. **Authors**: Eyal Trabelsi/Ehud Gudes [<a href="link">pdf</a>]

*Authors contributed equally to this paper.

## Building and The datasets

To create the IMDB dataset:

``` shell
make install_mock_imdb
```

To create the TPC-H dataset [go through this](https://github.com/tvondra/pg_tpch/blob/master/README.md)
