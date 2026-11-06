# Temporary remote soak host

Use one disposable native Linux host for the first remote pilot. Recommended
starting size: Hetzner CCX33, AMD64, eight dedicated vCPUs, 32 GB RAM and 240 GB
local disk. These are host resources; the fixture keeps its existing CPU/memory
limits. Confirm current capacity and prices in the project before creating it:
the public [plan page](https://www.hetzner.com/cloud/general-purpose/) does not
guarantee regional availability. Do not substitute shared CPUs silently.

The initial sequence is a 600-second smoke followed by a reviewed 7,200-second
pilot. No 24-hour run is started by this launcher. The pilot leaves snapshot
history (1,800 seconds), file retention (864,000 seconds) and automatic maintenance
unchanged. Read-only metadata/deletion-queue observations are enabled every minute.
They do not issue VACUUM or pause publication. Analyze those observations and
PostgreSQL logs to determine whether normal automatic maintenance keeps up;
a passing data check alone does not establish successful snapshot expiration.

## Local authentication

Create a Hetzner Cloud project for temporary tests and store its read/write API
token in 1Password. In the desktop app, copy the token field's **Secret Reference**.
Test using the installed `op` and `hcloud` CLIs:

```sh
HCLOUD_TOKEN='op://YOUR-VAULT/YOUR-ITEM/YOUR-FIELD' \
  op run -- hcloud server list
```

Repeat that prefix for provisioning commands. Only the secret reference goes
in shell history; 1Password supplies the token to the command at runtime.
No `hcloud context create` or persistent plaintext token is required.
Do not copy the Hetzner token to the remote host: it is unnecessary for the soak.
See [1Password's guide](https://www.1password.dev/cli/secrets-environment-variables)
and [hcloud setup](https://github.com/hetznercloud/cli/blob/main/docs/tutorials/setup-hcloud-cli.md).

## Provision the disposable host

Before creation, inspect `hcloud server-type describe ccx33`, locations, current
pricing and the project's SSH keys. Create a firewall allowing TCP port 22 only
from the administrator's public IPv4/IPv6 CIDRs. No database, engine or MinIO
ports need public access. Attach that firewall at creation, use a registered
SSH key and label the server `purpose=lake-soak`. The API credential stays local.

For example, after selecting the real key, firewall and available location:

```sh
HCLOUD_TOKEN='op://YOUR-VAULT/YOUR-ITEM/YOUR-FIELD' op run -- \
  hcloud server create --name lake-soak-beta5 --type ccx33 \
    --image ubuntu-24.04 --location hel1 \
    --ssh-key YOUR_REGISTERED_KEY --firewall YOUR_SSH_ONLY_FIREWALL \
    --label purpose=lake-soak --user-data-from-file deploy/soak/host.cloud-config.yml
```

The cloud-config installs Ubuntu's Docker/Compose packages, Git and Python;
it starts Docker but starts no workload. Wait for `cloud-init status --wait`.
If `/var/run/reboot-required` exists, reboot before continuing. Review service
status and record the chosen server ID/location/type. The host is billed until
deleted; a stopped server still incurs charges.

## Stage immutable inputs on the host

Clone this repository under `/opt/lake-soak/repository`, check out the explicit
committed runner revision and initialize `vendor/pg_lake` with full history:

```sh
git submodule update --init vendor/pg_lake
```

Transfer the accepted **AMD64** archive and its adjacent `.sha256` from CI run
`42201527552`, candidate `b0a703ab9f65a8f26904b21ed91c87df1944cdbc`, into
`dist/release/`. Preserve the original archive and checksum. Do not use the local
ARM64 archive, compile a replacement candidate or copy the uncommitted local
Dockerfile experiment. The host checkout must be clean.

The upstream prerequisite may take substantial time/disk to compile on a fresh
host. Either load a verified native prerequisite cache with
`tests/qualification/prerequisite.py load`, or build it using the committed
recipe before the pilot. A cache must include its matching identity manifest;
an arbitrary image tag is insufficient. Do not prune shared Docker resources.

```sh
python3 tests/qualification/prerequisite.py build
```

Run the preflight after that preparation, with the same arguments as the planned
job. It verifies archive checksum, candidate/architecture, clean runner/vendor,
local Docker, Compose, host resources and free space. With the defaults it needs
at least **80 GiB free** on both the checkout and Docker filesystems: 64 GiB
object budget plus 16 GiB reserve. This is an initial pilot ceiling, not a
prediction of growth or a validated day-long capacity. Builds also need space;
stage/build first and check available capacity again.

```sh
python3 deploy/soak/run.py preflight \
  --archive /opt/lake-soak/repository/dist/release/lake_replication-0.1.0-beta.5-linux-amd64.tar.gz \
  --candidate b0a703ab9f65a8f26904b21ed91c87df1944cdbc
```

## Run independently of SSH

On this private disposable host, launch a system service as root. This avoids
user-session lifetime and Docker-group setup. Keep the API token off this host.

```sh
sudo systemd-run --unit=lake-soak-smoke \
  --property=WorkingDirectory=/opt/lake-soak/repository \
  --property=RuntimeMaxSec=6h --property=TimeoutStopSec=180s \
  --property=KillSignal=SIGINT --property=KillMode=mixed \
  --property=Restart=no \
  /usr/bin/python3 /opt/lake-soak/repository/deploy/soak/run.py run \
    --archive /opt/lake-soak/repository/dist/release/lake_replication-0.1.0-beta.5-linux-amd64.tar.gz \
    --candidate b0a703ab9f65a8f26904b21ed91c87df1944cdbc \
    --duration-seconds 600
```

The six-hour service ceiling covers fresh fixture builds as well as the pilot;
the measured workload duration excludes image builds/setup. An exclusive lock
prevents overlapping launches through this wrapper. There is no automatic retry
after a failed run. `systemctl stop lake-soak-smoke` delivers SIGINT so the
harness records failure and attempts bounded cleanup. An abrupt host failure or
forced kill can still leave fixtures behind; inspect the recorded Compose
project and clean up that project specifically after preserving evidence.

Check `systemctl status lake-soak-smoke` and `journalctl -u lake-soak-smoke`.
Host identity and the full console stream are under `.qualification/remote_*`;
the console identifies the matching `.qualification/qualification_*_soak`
directory with results, resources, automatic-maintenance observations, events,
source snapshots and final logs. Evidence is outside the disposable Docker
volumes. The harness enforces a 16 GiB free-space reserve and 64 GiB object
ceiling; storage or backlog exhaustion fails the pilot rather than masking it.

After reviewing a successful smoke, repeat with a new unit name
`lake-soak-pilot` and `--duration-seconds 7200`. Review data, fault recovery,
automatic snapshot expiration, deletion queue, metadata/storage growth and
latency/resource observations before defining a 24-hour profile. Ten-day file
retention means a two-hour pilot cannot prove its eventual storage plateau.

## Preserve evidence and remove the host

Copy `.qualification/` to a private local evidence directory before deletion.
Record hashes of the retrieved files and the service exit result. The server
must remain available until transfer and evidence review complete. Then delete
the specifically recorded server with `hcloud server delete SERVER_ID`; inspect
and remove any test-owned primary IPs/firewall resources that remain. Do not
delete project-wide SSH keys or unrelated resources. No teardown is automatic
and no resource has been provisioned by adding this runbook.
