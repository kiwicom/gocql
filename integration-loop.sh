#!/bin/bash

for i in {1..100}; do
	echo "Trying $i"
	if ! env SCYLLA_IMAGE=scylladb/scylla:4.2.0 ./integration.sh cassandra scylla gocql_debug >"/tmp/integration-loop-$i.log" 2>&1; then
		exit $?
	fi
done

echo "Did not reproduce"
