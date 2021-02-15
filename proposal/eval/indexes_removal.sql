SELECT
    format('DROP INDEX %I.%I', n.nspname, c_ind.relname)
  FROM pg_index ind
  JOIN pg_class c_ind ON c_ind.oid = ind.indexrelid
  JOIN pg_namespace n ON n.oid = c_ind.relnamespace
  LEFT JOIN pg_constraint cons ON cons.conindid = ind.indexrelid
  WHERE
    n.nspname NOT IN ('pg_catalog','information_schema') AND
    n.nspname !~ '^pg_toast'::TEXT AND
    cons.oid IS NULL
