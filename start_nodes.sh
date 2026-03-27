#!/bin/bash

# This script starts N swarm localhost (127.0.0.1) nodes listening
# ports 9000, 9000+1, 9000+2, ..., 9000+N. All messages produced
# by all nodes are logged in run.log. The previous session's
# data points are moved to a backup file, and a fresh data.csv
# file is created by this script. At the end, the script outputs the
# list of swarm node processes under the current terminal.

# Kill all running nodes
pkill -f node.py

echo "Select destruction mode: random, fire, tornado"
read -p "Mode: " DESTRUCTION_MODE

# Backup the data from the previous (most recent) run in the backupdata folder
mkdir -p backupdata
mv data.csv backupdata/data-`date +%s`.csv
touch data.csv

# Erase the log from the most recent run
echo "" > run.log

# Validate input
if [[ "$DESTRUCTION_MODE" != "random" && "$DESTRUCTION_MODE" != "fire" && "$DESTRUCTION_MODE" != "tornado" ]]; then
    echo "Invalid mode. Defaulting to random."
    DESTRUCTION_MODE="random"
fi

# Number of nodes
N=49

# We start counting ports from 9000
PORT=9000

# Iterate from 1 (inclusive) until N (inclusive)
for row in 0 1 2 3 4 5 6; do
    for col in 0 1 2 3 4 5 6; do
        PORT=$((9000 + row * 7 + col))
        python3 -u node.py $PORT $N $DESTRUCTION_MODE>> run.log 2>&1 &
        sleep 0.1
    done
done
# List the processes currently running nodes
ps -ef | grep node.py | grep -v grep
