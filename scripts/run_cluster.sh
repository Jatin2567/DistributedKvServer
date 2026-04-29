#!/bin/bash

echo "Starting cluster..."

go run cmd/server/main.go configs/node1.json &
PID1=$!

go run cmd/server/main.go configs/node2.json &
PID2=$!

go run cmd/server/main.go configs/node3.json &
PID3=$!

echo "Nodes started:"
echo "node1 → :8081"
echo "node2 → :8082"
echo "node3 → :8083"

echo "Press Ctrl+C to stop..."

trap "kill $PID1 $PID2 $PID3" EXIT
wait