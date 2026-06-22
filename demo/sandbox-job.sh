#!/bin/sh
# Sandbox wrapper for executing an untrusted job.
#
# Invoked by the runner (via SAGA) as:
#     sandbox-job.sh <working_directory> <executable> [args...]
#
# It runs <executable> [args...] inside bubblewrap with:
#   - no network (--unshare-net, via --unshare-all);
#   - its own user/mount/PID/IPC/UTS/cgroup namespaces;
#   - a fresh tmpfs root: only the read-only simulator environment and the job's
#     own working directory are visible, so the job cannot read the provider's
#     config/token, the TLS keys, or any other job's directory;
#   - resource limits (max processes, CPU seconds, no core dumps) and a
#     wall-clock timeout.
#
# Requires unprivileged user namespaces to be available (see demo/k8s/README.md).
set -eu

if [ "$#" -lt 2 ]; then
    echo "usage: $0 <working_directory> <executable> [args...]" >&2
    exit 2
fi

WORKDIR=$1
shift   # remaining arguments are the executable and its arguments

TIMEOUT=${SANDBOX_TIMEOUT:-600}            # wall-clock seconds
NPROC=${SANDBOX_NPROC:-512}                # max processes (fork-bomb guard)
CPU_SECONDS=${SANDBOX_CPU_SECONDS:-1200}   # CPU seconds
SIM_ENV=${SANDBOX_SIM_ENV:-/home/docker/env}

# Resource limits applied to the job process tree only (this wrapper is the job
# process that SAGA forks; the runner is a separate process). Memory is bounded
# by the pod's cgroup limit, so we do not set an address-space rlimit (which can
# spuriously break simulators that reserve large virtual mappings).
ulimit -u "$NPROC"       2>/dev/null || true
ulimit -t "$CPU_SECONDS" 2>/dev/null || true
ulimit -c 0              2>/dev/null || true

exec timeout --kill-after=10 --signal=TERM "$TIMEOUT" \
    bwrap \
        --unshare-all \
        --die-with-parent \
        --new-session \
        --ro-bind /usr /usr \
        --ro-bind-try /bin /bin \
        --ro-bind-try /sbin /sbin \
        --ro-bind-try /lib /lib \
        --ro-bind-try /lib64 /lib64 \
        --ro-bind-try /etc/ssl /etc/ssl \
        --ro-bind-try /etc/ld.so.cache /etc/ld.so.cache \
        --ro-bind-try /etc/nsswitch.conf /etc/nsswitch.conf \
        --ro-bind-try /etc/passwd /etc/passwd \
        --ro-bind "$SIM_ENV" "$SIM_ENV" \
        --bind "$WORKDIR" "$WORKDIR" \
        --proc /proc \
        --dev /dev \
        --tmpfs /tmp \
        --chdir "$WORKDIR" \
        --setenv HOME /tmp \
        "$@"
