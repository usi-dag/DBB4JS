#!/bin/bash

mkdir -p results

export t=$(date -d "today" +"%Y-%m-%d:%H.%M")


maybenuma() {
  if hash numactl 2>/dev/null; then
    numactl --cpubind=0 --membind=0 $@
  else
    $@
  fi
}

bench="benchStreamQueryOpt$|benchImperativeQuery$"
fname="./results/bp-all-$t.json"
maybenuma $JAVA_HOME/bin/java -jar  -Djmh.ignoreLock=true ./target/benchmark.jar $bench -f 2 -w 10 -wi 5 -i 5 -r 10 -bm avgt -tu ms -rf JSON -rff $fname

RESULT=$?
if [ $RESULT -eq 0 ]; then
  cp $fname ./results/last-all.json
  echo "Out: ./results/bp-$t.json"
else
  echo "Run Failed"
fi

