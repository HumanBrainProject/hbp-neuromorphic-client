# Demo provider — Kubernetes deployment (Tier 0 hardening)

Manifests for running the Demo neuromorphic provider as a hardened Kubernetes pod.
This is the **Tier 0** step of the security plan: it does not yet sandbox individual
jobs (Tier 1), but it removes root, bounds resources, restricts egress, and stops
serving untrusted output as active content.

## What's here

| File | Purpose |
|---|---|
| `deployment.yaml` | Runner container (executes jobs) + read-only nginx `fileserver` sidecar |
| `data-pvc.yaml` | Persistent volume for job output (`/home/docker/data`) |
| `fileserver-configmap.yaml` | Hardened nginx config (no autoindex, forced download, `nosniff`) |
| `service.yaml` | ClusterIP exposing the file server on port 80 → 8080 |
| `networkpolicy.yaml` | Default-deny ingress/egress with a narrow allowlist |
| `ingress.yaml` | Example TLS ingress for `demo.hbpneuromorphic.eu/data` |
| `secret.example.yaml` | Template for the runtime `nmpi.cfg` (holds `AUTH_TOKEN`) |
| `kustomization.yaml` | Bundles everything except the Secret |

## Deploy

```sh
# 1. Create the config Secret (kept out of git)
cp secret.example.yaml secret.yaml
# edit secret.yaml: set AUTH_TOKEN
kubectl apply -f secret.yaml

# 2. Apply the rest
kubectl apply -k .
```

## Hardening applied (Tier 0)

- **No root.** The image no longer ships supervisor/cron/nginx; the runner is the
  container's main process (`run-loop.sh`) running as the non-root `docker` user.
  Pod sets `runAsNonRoot`, drops **all** capabilities, `allowPrivilegeEscalation:
  false`, `readOnlyRootFilesystem: true`, and `seccompProfile: RuntimeDefault`.
- **Bounded resources.** CPU/memory/ephemeral-storage limits cap runaway or abusive
  jobs (crypto-mining, fork bombs, disk fill).
- **Restricted network.** Default-deny `NetworkPolicy`; egress allowed only to public
  HTTP(S)/git, with the cloud metadata endpoint (`169.254.169.254`) and all private
  ranges blocked. `automountServiceAccountToken: false` keeps the job away from the
  Kubernetes API.
- **Safe output serving.** The file server mounts the data volume **read-only**, with
  directory listing off and every file served as a non-sniffable attachment, scoped
  CORS instead of `*`.

## Operator notes / prerequisites

- **`runAsUser: 1000`** in `deployment.yaml` must match the `docker` user baked into
  the base image. Verify and adjust:
  `docker run --rm docker-registry.ebrains.eu/neuromorphic/demo id`.
- The **egress `NetworkPolicy` only takes effect if the CNI enforces it** (Calico,
  Cilium, etc.). Confirm with your cluster.
- A **per-pod PID limit** is a node-level kubelet setting (`podPidsLimit`), not
  expressible in this manifest — set it on the node pool for fork-bomb protection.
- **TLS** is terminated at the ingress (cert-manager in the example), replacing the
  in-pod letsencrypt mount the old `docker run` setup used.

## Not yet covered (Tier 1+)

Per-job sandboxing (bubblewrap/nsjail, no job-time network, no access to the config
Secret), and fixes for `git clone --recursive` submodule SSRF and zip-slip archive
extraction. See the security plan for details.
