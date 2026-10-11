# vpn-data-platform-apps

What runs on the `vpn-data-platform` cluster, as Argo CD reads it.
Infrastructure (VPC, EKS, RDS, IAM, Argo CD itself) is in
[VPN_DATA_PLATFORM](https://github.com/nopega/VPN_DATA_PLATFORM); this repo
holds nothing that touches AWS and no credentials.

```
apps/        one Argo CD Application per component — the `root` Application watches this folder
next/        Applications written but not enabled yet (root does not look here)
charts/      Helm charts, vendored: polaris 1.5.0, trino 1.42.2 (Trino 480)
workloads/   plain manifests for things that are not charts
```

## How it is connected

`root` is created by Terraform (`VPN_DATA_PLATFORM/terraform/05_argocd`), in
the same apply that installs Argo CD, and points at `apps/` here. A fresh
cluster therefore needs no manual step: Argo CD starts, reads this folder,
and installs everything in it in sync-wave order.

```
git push  →  root sees apps/ changed  →  creates/updates that Application
          →  the Application deploys its chart or manifests
```

## Order

| Wave | Application | Namespace | Needs |
|---|---|---|---|
| -20 | `polaris` | polaris | `bootstrap/polaris.sh` (database, secret, realm) |
| 0 | `smoke-test` | smoke-test | — (nginx at test.nopega.net) |
| 10 | `trino` *(in next/)* | trino | `bootstrap/trino.sh`, catalog from `bootstrap/polaris_catalog.sh` |

Waves really wait: the infra repo configures Argo CD with a health check for
Application objects, so wave 10 starts only once wave -20 reports Healthy.
(Without that check — the case in the original platform — waves only order
creation.)

## Enabling something from next/

Its bootstrap script (in the infra repo) runs first, because secrets never
come from git. Then:

```bash
git mv next/<name>.yaml apps/      # or plain mv if never committed
git commit -m "Enable <name>" && git push
```
