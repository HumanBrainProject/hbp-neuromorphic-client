#!/bin/sh
# Entry point for the Demo provider runner pod.
#
# In the previous (docker-run) deployment a cron job invoked nmpi_run.py every
# minute. Under Kubernetes the runner is the container's main process instead, so
# we loop here. One nmpi_run.py invocation drains all currently-queued jobs, then
# we sleep before polling again. SIGTERM/SIGINT (sent by Kubernetes on pod
# shutdown) is honoured between iterations for a clean exit.
set -u

INTERVAL="${POLL_INTERVAL:-60}"
term=0
trap 'term=1' TERM INT

echo "Starting NMPI Demo runner loop (poll interval ${INTERVAL}s)"
while [ "$term" -eq 0 ]; do
    /usr/bin/python3 /home/docker/bin/nmpi_run.py || echo "nmpi_run.py exited non-zero (continuing)"
    [ "$term" -eq 0 ] && sleep "$INTERVAL"
done
echo "Received termination signal, exiting."
