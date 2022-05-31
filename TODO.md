1) Support athena parser:
    a. Remove the remote from parsing
    b. check limit query
    c. support redundent_operation
2) Fix documentation to work:
   - Jupyter examples
     - make it work with a mock
     - https://nbviewer.org/
3) better repository
   - Usage / Docs
     - readthedocs
     - how it should be used
   - lean installation per database
   - support terminal execution
   - Support easier installations:
     - pypi
     - apt-get
     - brew
4) Refactor to introduce new abstractions:
- Flow/Execution Plan abstraction ast, metrics
- refactor enrichment_stats
- refactor_logging like matplotlib
5) multiple_queries_grouping -  Support grouping edges with the same start end nodes, and represent them by either avg/median/max/min.
6) fix_zero_edge_bug - UI edge size when hovering .replace(0, 1)
8supporting_new_parsers:
- mysql
- sqlite
- presto
- spark
