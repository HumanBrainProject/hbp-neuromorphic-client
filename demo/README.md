This folder contains files for a demonstration "neuromorphic computing service provider",
which accepts PyNN jobs, but just runs them with NEST.

The queue name is "Demo".

Because the Demo executes user-submitted Python, it is deployed as a hardened
Kubernetes pod rather than a plain container. The image is now runner-only (no
supervisor/cron/nginx, never root); the runner is the container's main process and
job output is served by a separate read-only nginx sidecar.

Deployment manifests and instructions are in `k8s/` (see `k8s/README.md`):

    cp k8s/secret.example.yaml k8s/secret.yaml   # set AUTH_TOKEN
    kubectl apply -f k8s/secret.yaml
    kubectl apply -k k8s/

For local development you can still run the image directly; it polls the queue using
the config mounted at /home/docker/config/nmpi.cfg:

    docker run --rm -v "$PWD/nmpi.cfg:/home/docker/config/nmpi.cfg:ro" \
        docker-registry.ebrains.eu/neuromorphic/demo