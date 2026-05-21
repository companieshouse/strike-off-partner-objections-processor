#!/bin/bash
#
# Start script for strike-off-partner-objections-processor

PORT=8080

exec java -jar -Dserver.port="${PORT}" "strike-off-partner-objections-processor.jar"