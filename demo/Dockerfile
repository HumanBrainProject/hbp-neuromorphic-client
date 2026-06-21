FROM docker-registry.ebrains.eu/neuralensemble/simulation:pynn0110

# Only the tools needed to fetch job code and sandbox its execution. Under
# Kubernetes the runner is the container's main process (see run-loop.sh) and
# file serving is handled by a separate pod, so supervisor/cron/nginx are no
# longer installed here and this image never runs anything as root.
# bubblewrap sandboxes each untrusted job (see sandbox-job.sh); coreutils
# provides `timeout` for the wall-clock limit.
USER root
RUN apt-get update --fix-missing; \
    apt-get -y -q install python3-pip git unzip curl bubblewrap coreutils; \
    rm -rf /var/lib/apt/lists/*
USER docker

COPY requirements.txt /home/docker/packages/
COPY requirements-provider.txt /home/docker/packages/
# we access the NMPI client from the system Python, not the virtual env the simulator runs in.
# As a non-root user, pip installs into the user site (/home/docker/.local).
RUN /usr/bin/python3 -m pip install -r /home/docker/packages/requirements.txt
RUN /usr/bin/python3 -m pip install -r /home/docker/packages/requirements-provider.txt

RUN mkdir -p /home/docker/runner /home/docker/bin
COPY nmpi /home/docker/packages/nmpi
COPY bin/nmpi_run.py /home/docker/bin/nmpi_run.py
COPY demo/run-loop.sh /home/docker/bin/run-loop.sh
COPY demo/sandbox-job.sh /home/docker/bin/sandbox-job.sh
RUN chmod +x /home/docker/bin/run-loop.sh /home/docker/bin/sandbox-job.sh

# The root filesystem is mounted read-only in Kubernetes, so HOME points at a
# writable emptyDir volume. PYTHONUSERBASE keeps the user-installed packages
# (built into /home/docker/.local above) resolvable despite the HOME change.
ENV PYTHONPATH=/home/docker/packages \
    NMPI_CONFIG=/home/docker/config/nmpi.cfg \
    PYTHONUSERBASE=/home/docker/.local \
    HOME=/home/docker/state \
    MPLCONFIGDIR=/tmp/matplotlib \
    PYTHONDONTWRITEBYTECODE=1 \
    POLL_INTERVAL=60 \
    SANDBOX_SIM_ENV=/home/docker/env \
    SANDBOX_TIMEOUT=600 \
    SANDBOX_NPROC=512 \
    SANDBOX_CPU_SECONDS=1200

# cwd must be writable; the runner writes per-job directories under here.
WORKDIR /home/docker/runner

# Poll the job queue forever; one nmpi_run.py invocation takes MAX_CONCURRENT_JOBS from the queue.
CMD ["/home/docker/bin/run-loop.sh"]