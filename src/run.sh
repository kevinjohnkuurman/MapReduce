#!/usr/bin/env bash

n_instances=$1
pids=""

# start the workers
for (( i = 0; i < $n_instances; i++ )); do
    python3 main.py -i $i -w $n_instances &
    pids="$pids $!"
done

# trap the ctrl_c command
trap ctrl_c INT
function ctrl_c() {
    echo "** Killing running instances"
    for pid in $pids; do
        kill $pid
    done
}

# wait for all pids
wait $pids