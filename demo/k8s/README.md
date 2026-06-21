# Demo provider — Kubernetes deployment (Tier 0 + Tier 1 hardening)

Manifests for running the Demo neuromorphic provider as a hardened Kubernetes pod.
**Tier 0** removes root, bounds resources, restricts egress, and stops serving
untrusted output as active content. **Tier 1** additionally sandboxes each
individual job (no network, isolated namespaces, resource/time limits) and fixes
the concrete code-fetch vulnerabilities.

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
  CORS (`*.ebrains.eu` / `*.hbpneuromorphic.eu`) instead of `*`.

## Hardening applied (Tier 1)

- **Per-job sandbox.** Each untrusted job runs inside bubblewrap (`sandbox-job.sh`,
  enabled via `SANDBOX_WRAPPER` in the config): **no network**, its own
  user/mount/PID/IPC namespaces, a fresh tmpfs root exposing only the read-only
  simulator env and the job's own working directory — so a job cannot read the
  provider's config/token, the TLS material, or any other job's files. Resource
  limits (max processes, CPU seconds) and a wall-clock `timeout` bound each job.
- **Code-fetch fixes.** `git clone` no longer uses `--recursive` (submodule URLs are
  attacker-controlled — SSRF) and runs shallow, non-interactive, with a timeout.
  Archives are unpacked with a zip-slip/zip-bomb-safe extractor that rejects
  absolute paths, `..` traversal, symlinks/devices, and oversized expansion. Inline
  scripts are size-capped, and an optional `ALLOWED_CODE_HOSTS` allowlist restricts
  fetch hosts.
- **Ephemeral workdirs.** With `CLEANUP_WORKDIR=True`, each job's directory is
  removed once its output is collected, so jobs cannot read each other's leftovers.
- **Fail-closed.** If a job cannot be set up or sandboxed (e.g. user namespaces
  unavailable, unreachable repo, oversized script) the runner marks that job failed
  and continues, rather than executing it unsandboxed or stalling the queue.

## Operator notes / prerequisites

- **`runAsUser: 1000`** in `deployment.yaml` must match the `docker` user baked into
  the base image. Verify and adjust:
  `docker run --rm docker-registry.ebrains.eu/neuromorphic/demo id`.
- **Unprivileged user namespaces** must be available on the node for the bubblewrap
  sandbox to start (most modern GKE/EKS/AKS node images allow them under
  `RuntimeDefault` seccomp). Verify inside the pod:
  `kubectl exec deploy/nmpi-demo -c runner -- bwrap --unshare-all --dev /dev --ro-bind /usr /usr echo ok`
  (should print `ok`). If it is blocked, either enable
  `kernel.unprivileged_userns_clone=1` on the nodes or attach a custom (localhost)
  seccomp profile that permits the namespace syscalls. Jobs fail closed until then.
- The **egress `NetworkPolicy` only takes effect if the CNI enforces it** (Calico,
  Cilium, etc.). Confirm with your cluster.
- A **per-pod PID limit** is a node-level kubelet setting (`podPidsLimit`), not
  expressible in this manifest — set it on the node pool for fork-bomb protection.
- **TLS** is terminated at the ingress (cert-manager in the example), replacing the
  in-pod letsencrypt mount the old `docker run` setup used.

## Not yet covered (Tier 2+)

Optional sandboxed RuntimeClass (gVisor/Kata) for VM/syscall-level isolation, and
per-job ephemeral pods / microVMs.
