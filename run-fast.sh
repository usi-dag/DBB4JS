#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

mkdir -p results

if [ "$1" ]; then
  export bench="$1"
else
  export bench="Query"
fi


export t=$(date -d "today" +"%Y-%m-%d:%H.%M")


maybenuma() {
  if hash numactl 2>/dev/null; then
    numactl --cpubind=0 --membind=0 $@
  else
    $@
  fi
}

maybenuma $JAVA_HOME/bin/java -jar  -Djmh.ignoreLock=true $SCRIPT_DIR/target/benchmark.jar $bench -f 1 -w 3 -wi 3 -i 3 -r 3 -bm avgt -tu ms -rf JSON -rff ./results/bp-$t.json

RESULT=$?
if [ $RESULT -eq 0 ]; then
  cp ./results/bp-$t.json ./results/last.json
  echo "Out: ./results/bp-$t.json"
else
  echo "Run Failed"
fi

