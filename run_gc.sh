#!/bin/bash

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

maybenuma $JAVA_HOME/bin/java -jar  -Djmh.ignoreLock=true ./target/benchmark.jar $bench -f 2 -w 10 -wi 5 -i 5 -r 10 -bm avgt -tu ms -prof gc -rf JSON -rff ./results/gc/bp-$t.json

RESULT=$?
if [ $RESULT -eq 0 ]; then
  cp ./results/bp-$t.json ./results/last.json
  echo "Out: ./results/bp-$t.json"
else
  echo "Run Failed"
fi

