#!/bin/bash

scale=${1-1}
create_db=${2:1}

db="tpch-benchmark-scale-$scale"
optimized_db="$db-optimized"
benchmarkDir="/Users/etrabelsi/$db"
queriesDir="$benchmarkDir/queries"

if [[ -n "$2" ]]; then
  if psql -lqt | cut -d \| -f 1 | grep -qw $db; then
      echo "drop db $db "
      dropdb $db
      echo "drop db $optimized_db "
      dropdb $optimized_db
  fi

  echo "create $db db"
  createdb $db
  echo "create $optimized_db db"
  createdb $optimized_db

  psql  $db -f dss.ddl
  psql  $optimized_db -f dss.ddl

  sudo ./dbgen -vf -s $scale


  mkdir -p $benchmarkDir

  for i in `ls *.tbl`; do
    table=${i/.tbl/}
    echo "Moving $i to $benchmarkDir/$i"
    mv -f $i "$benchmarkDir/$i"

    echo "Loading $table..."
    psql $db -q -c "TRUNCATE $table"
    psql $db -c "\\copy $table FROM '$benchmarkDir/$i' CSV DELIMITER '|'"

    psql $optimized_db -q -c "TRUNCATE $table"
    psql $optimized_db -c "\\copy $table FROM '$benchmarkDir/$i' CSV DELIMITER '|'"

  done

  mkdir -p $queriesDir
  for i in `ls queries/*.sql`; do
    tail -r $i | sed '2s/;//' | tail -r > "$benchmarkDir/$i"
  done

  sudo DSS_QUERY=$queriesDir ./qgen | sed 's/limit -1//' | sed 's/day (3)/day/' > "$queriesDir/queries.sql"

  psql $db < "../../indexes.sql"
  psql $db -c "ANALYZE VERBOSE"
  psql $optimized_db < "../../indexes.sql"
  psql $optimized_db -c "ANALYZE VERBOSE"

  #dexter $db "$queriesDir/queries.sql" --input-format sql >> "$queriesDir/indexes.sql"
  #dexter $db "$queriesDir/queries.sql" --input-format sql --create
  psql $optimized_db < "../../configurations.sql"

  #  perl postgresqltuner.pl --host="localhost" --database=$db >> "$benchmarkDir/postgresqltuner.out"
  #  perl postgresqltuner.pl --host="localhost" --database=$optimized_db >> "/$benchmarkDir/postgresqltuner-optimized.out"

  brew services restart postgresql
  sleep 10

fi


time psql $db -o /dev/null < "$queriesDir/queries.sql"
time psql $optimized_db -o /dev/null < "$queriesDir/queries.sql"
