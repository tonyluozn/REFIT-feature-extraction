#!/bin/bash

set -e

if [[ "${FLINK_JOB_MODE}" == "streaming" ]]; then
  echo "Starting Flink in session mode for streaming jobs..."
  /docker-entrypoint.sh jobmanager
else
  echo "Starting Flink in session mode for batch jobs..."
  /docker-entrypoint.sh taskmanager
fi
