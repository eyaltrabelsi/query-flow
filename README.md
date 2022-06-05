# QueryFlow


[comment]: <> ([![pypi]&#40;https://img.shields.io/pypi/v/query-flow.svg&#41;]&#40;https://pypi.org/project/query-flow/&#41;)

[comment]: <> ([![python]&#40;https://img.shields.io/pypi/pyversions/query-flow.svg&#41;]&#40;https://pypi.org/project/query-flow/&#41;)

[comment]: <> ([![Build Status]&#40;https://github.com/eyaltrabelsi/query-flow/actions/workflows/dev.yml/badge.svg&#41;]&#40;https://github.com/eyaltrabelsi/query-flow/actions/workflows/dev.yml&#41;)

QueryFlow, is a query visualization tool that provides insights into common problems in your SQL query.
QueryFlow visualizes the query execution using the Sankey diagram, a technique that allows one to illustrate complex processes, with a focus on a single aspect or resource that you want to highlight.
This allow to tackle the following problems:

* [Identifying missing records.](https://github.com/eyaltrabelsi/query-flow/blob/master/examples/Identifying%20the%20operation%20that%20caused%20the%20return%20of%20zero%20records%20.ipynb)
* [Identifying Ineffective operations.](https://github.com/eyaltrabelsi/query-flow/blob/master/examples/Identifying%20Ineffective%20operations.ipynb)
* [Identifying duplications in a query.](https://github.com/eyaltrabelsi/query-flow/blob/master/examples/Identifying%20duplications.ipynb)
* Comparing optimizer planned metrics to actual metrics.
* Identifying performance bottlenecks in a single query.
* Identifying performance bottlenecks in multiple queries.

Currently QueryFlow support the following databases/data-engines:
* Athena
* PostgreSQL

* Documentation: <https://eyaltrabelsi.github.io/query-flow>
* GitHub: <https://github.com/eyaltrabelsi/query-flow>
* PyPI: <https://pypi.org/project/query-flow/>
* Free software: MIT


## Installing #
The best way to install query-flow is:
```
$ pip install query-flow
```
In case you want to use another way go to the [installation page.](https://eyaltrabelsi.github.io/query-flow/installation/)


[comment]: <> (## Publications #)

[comment]: <> (**Title**: Visualizing Database Execution Plans using)

[comment]: <> (Sankey. **Authors**: Eyal Trabelsi/Ehud Gudes [<a href="link">pdf</a>])

[comment]: <> (*Authors contributed equally to this paper.)
