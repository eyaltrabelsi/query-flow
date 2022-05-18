1) Support athena parser:
    a. Remove the remote from parsing
    b. support from_query using boto
    c. support redundent_operation
2) Fix documentation to work:
   - Jupyter examples
   - test examples
3) better repository
   - Installation documentation
   - Usage / Docs
   - Examples
   - support terminal execution
   - lean installation per database
   - Support easier installations:
     - pypi
     - apt-get
     - brew
4) Refactor to introduce new abstractions:
- Flow/Execution Plan abstraction ast, metrics
- refactor enrichment_stats
- refactor_logging like matplotlib
5) multiple_queries_grouping -  Support grouping edges with the same start end nodes, and represent them by either avg/median/max/min.
6fix_zero_edge_bug - UI edge size when hovering .replace(0, 1)
7support base queries
8supporting_new_parsers:
- mysql
- sqlite
- presto
- spark