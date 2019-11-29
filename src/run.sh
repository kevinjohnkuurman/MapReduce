#!/usr/bin/env bash

n_workers=$1
pids=""

# start the server
python3 main.py -i 0 -w $n_workers &
pids="$pids $!"

# start the workers
for (( i = 1; i <= $n_workers; i++ )); do
    python3 main.py -i $i -w $n_workers &
    pids="$pids $!"
done

# wait for all pids
wait $pids