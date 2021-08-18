1) fix_wrong_unique - Unique work wrong with verbose=False 4. Identifying Ineffective operations.ipynb.
   fix_index_naming of the scan

2) Refactor to introduce new abstractions:
- Flow/Execution Plan abstraction
- refactor enrichment_stats

3) support_terminal_execution

4) refactor_logging like matplotlib.

5) multiple_queries_grouping -  Support grouping edges with the same start end nodes, and represent them by either avg/median/max/min.

6) update_readme:
- Installation
- Usage / Docs
- Examples

7) fix_zero_edge_bug - UI Bug due to .replace(0, 1)

8) Support easier installations:
- pypi
- apt-get
- brew

9) supporting_new_parsers:
- mysql
- sqlite

14) optimization_insights:
- [slow scan](https://pganalyze.com/docs/explain/insights/slow-scan).
- [inefficient index](https://pganalyze.com/docs/explain/insights/inefficient-index).
- [Hash batches](https://pganalyze.com/docs/explain/insights/hash-batches).
- [Disk Sorts](https://pganalyze.com/docs/explain/insights/disk-sort).
- [Lossy Bitmaps](https://pganalyze.com/docs/explain/insights/lossy-bitmaps).
- [Stale stats](https://pganalyze.com/docs/explain/insights/stale-stats)- planner missed by a factor of X or more.
- [I/O Heavy](https://pganalyze.com/docs/explain/insights/io-heavy).
- Keep results of CTE small (keep big JOINs outside).
- Unused indexes.
- Nested loop should work only on small tables.
- Merge join is good for two big tables (sort), we can use index to mitigate this.
- Hash Join scan one table and build hash table (high memory), for big table it won’t be optimal.
  If stuff getting pulled from heap we might need to tune shared_buffers (can check explain analyse)