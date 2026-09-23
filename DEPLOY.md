# Deploying notebook_spotter

Runs as a container: Python 3.12 + Chromium + chromedriver, kept alive by systemd.

## Do not deploy on a cPanel/WHM server

Tried on `firsthosts.hypergroup.com.tn` (AlmaLinux 8 + cPanel) and rolled back. cPanel's
**VirtFS** replicates `/var`, `/opt`, `/usr` and `/home` into a jail for every hosted
account. It picks up podman's overlay mounts under `/var/lib/containers` and bind-mounts
them into all of those jails — 52 of 59 accounts, 156 stale mounts in one short run.

Worse, those jail bind-mounts hold the overlay open, so podman cannot clean up after a
container dies:

```
Error: removing storage for container "notebook_spotter":
       replacing mount point ".../merged": device or resource busy
Error: creating container storage: the container name "notebook_spotter" is already in use
```

The name stays taken and every restart fails with exit 125 — an unrecoverable crash loop.
`--replace` alone does not fix it, because the underlying unmount is what fails.

Use a host without VirtFS. Any plain VPS works.

## Requirements

- podman or docker
- systemd
- outbound HTTPS to `amazon.{de,it,es,fr,co.uk}` and `api.telegram.org`
- ~1.5 GB disk for the image
- RAM: settles around 4 GB. Measured on a 4-core / 7.6 GB host (2 GB swap added as
  headroom): climbs over the first ten minutes, then plateaus. It is Chrome churn, not a
  leak, but size the host for 4 GB plus headroom — a plain 8 GB VPS has room to spare.
- CPU: roughly 1 to 1.5 cores. A 4-core box keeps cycles finishing well inside the 10s
  interval.

## Install

```bash
mkdir -p /opt/notebook_spotter/state
cp main.py requirements.txt Containerfile watchdog.py /opt/notebook_spotter/
cp known_listings.json /opt/notebook_spotter/state/     # omit to start with an empty history

# Telegram credentials live outside the image and outside git — see Configuration below
cat > /opt/notebook_spotter/telegram.env <<'EOF'
TELEGRAM_BOT_TOKEN=<your bot token>
TELEGRAM_CHAT_ID=<your chat id>
EOF
chmod 600 /opt/notebook_spotter/telegram.env

# The systemd unit runs as root, so build as root too — podman's image storage is
# per-user, and a rootless build under another account is invisible to the root unit.
cd /opt/notebook_spotter && sudo podman build -t notebook_spotter:latest .

sudo cp notebook_spotter.service notebook_spotter-watchdog.service notebook_spotter-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now notebook_spotter.service
sudo systemctl enable --now notebook_spotter-watchdog.timer
sudo journalctl -u notebook_spotter -f
```

Startup takes 2-3 minutes: the five browsers are bootstrapped one at a time because the
delivery-address popover is unreliable when they race each other. `Ready | 5/5 domains
live` means it is watching. If `known_listings.json` already had entries, this first run
resumes silently (no digest) — a digest only fires from a genuinely empty state file.

## Configuration

Set in `telegram.env` (credentials) or the unit / Containerfile (everything else); all
except the Telegram vars have working defaults.

| Variable | Default | Purpose |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | *(required, no default)* | Telegram bot token — set in `telegram.env`, never hardcoded |
| `TELEGRAM_CHAT_ID` | *(required, no default)* | Chat/channel id to post to — same file |
| `CHROME_PATH` | `/usr/bin/chromium` (Linux), Chrome's path on Windows | browser binary |
| `CHROMEDRIVER_PATH` | `/usr/bin/chromedriver` in the image, else Selenium Manager | driver binary |
| `STATE_DIR` | next to `main.py` | where `known_listings.json` and the lock live |

Polling rate is `REFRESH_INTERVAL` in `main.py` (10s). Raising it to 30s cuts CPU roughly
threefold, at the cost of alert latency.

`telegram.env` is `chmod 600`, lives only on the host under `/opt/notebook_spotter/`, and
is `.gitignore`'d — it must never be committed. Both `notebook_spotter.service` (via
`--env-file`) and `notebook_spotter-watchdog.service` (via `EnvironmentFile=`) read the
same file, so there is only ever one copy of the real token on disk.

## Operating it

```bash
sudo systemctl stop notebook_spotter        # frees all CPU and RAM immediately
sudo systemctl disable notebook_spotter     # and stop it coming back at boot
sudo journalctl -u notebook_spotter -f      # live log
```

`systemctl stop` sends `SIGTERM`. `main.py` installs a handler for it that raises the same
clean-shutdown path as Ctrl-C, so every Chrome process this run opened is closed and the
lock file is released — without that handler, `SIGTERM` bypasses Python's `finally` block
entirely and orphans every Chrome process still open, which is exactly how a laptop dev
box once got its RAM eaten alive across a few manual restarts. Do not send `SIGKILL`
(`kill -9`) to stop it normally; that still bypasses cleanup.

The unit sets `Restart=always`, `RestartSec=15` and `StartLimitBurst=0`, so it comes back
from any crash, OOM or reboot, without systemd ever giving up.

`StartLimitIntervalSec`/`StartLimitBurst` must sit in `[Unit]`, not `[Service]` — systemd
239 (EL8) silently ignores them in `[Service]`, and the default limit of 5 starts per 10s
would then latch the service off for good after a crash loop.

`--memory` and `--cpus` in the unit are hard ceilings, worth keeping on any shared box.
`--shm-size=1g` is not optional: five Chromes on the default 64 MB `/dev/shm` crash.
`--network=slirp4netns` keeps networking in userspace, so no bridge interface and no
iptables NAT rules are added to the host; the container only ever dials out.

`--log-driver=none` is deliberate: the container's output is already attached to the
unit's stdout and reaches the journal that way. Podman's own journald driver would write
every line a second time.

## Watchdog

`watchdog.py` runs from `notebook_spotter-watchdog.timer` every 60s and reports to the
same Telegram chat. It lives on the host, not in the container, because a watchdog that
shares a process with the thing it watches dies with it. It reads
`TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` from the same `telegram.env` via
`EnvironmentFile=` in its unit — there is no copy of the token inside `watchdog.py`.

| Alert | Fires when |
|---|---|
| 🔴 PROBLEM | the unit is not active, or is active but has not completed a cycle in 5 min |
| 🟢 recovered | it comes back after a problem |
| ⚠️ crashed and restarted | `NRestarts` went up — `Restart=always` heals a crash in seconds, so this is the only way you would ever learn it happened |
| ✅ daily check | once every 24h, so total silence means the box itself is gone |

Only transitions are announced: something that stays down does not repeat the message every
minute. State lives in `state/watchdog.json`; delete it to reset (the next run then re-sends
the heartbeat).

A restart takes ~3 min to bootstrap five browsers before the first cycle is logged, so the
staleness check is skipped for the first 7 minutes of any run. Otherwise every legitimate
restart would raise a false alarm.

```bash
sudo python3 /opt/notebook_spotter/watchdog.py          # run a check by hand
sudo journalctl -u notebook_spotter-watchdog -f          # watch it
sudo systemctl disable --now notebook_spotter-watchdog.timer   # turn alerting off
```

## Alert latency

Cycles start every 10s (`REFRESH_INTERVAL`) and each domain alerts the moment it reports,
without waiting for the slowest marketplace. A new listing or a price drop therefore
reaches Telegram about 8-16s after it appears on the page being watched.

That is the floor, not a guarantee: only page 1 of the price-ascending results is read, so
anything priced above that page is never seen at all, however fast the polling.

## Search URLs

Each marketplace has its own Warehouse node and its own Notebooks category, and all three
parameters have to agree or Amazon silently ignores the category filter and serves the
wrong department:

| | srs / bbn (Warehouse) | rh=n: (Notebooks) |
|---|---|---|
| DE | 3581963031 | 427957031 |
| IT | 3581999031 | 460158031 |
| ES | 3582001031 | 938008031 |
| FR | 3581943031 | 429879031 |
| UK | 3581866031 | 429886031 |

Unlike the memory-only search this project started as, the Notebooks category has no
`k=` keyword to narrow it further — it's a browse of the whole department. That makes it
leakier: mechanical keyboards, monitors, a robot vacuum, a software licence and a stylus
have all shown up warehouse-priced under it. `is_notebook()` in `main.py` compensates with
a two-sided check — a title must contain an actual laptop/notebook word (multi-language)
*and* not match a broad accessory/spare-parts exclusion list — rather than trusting the
category alone.

To find these for a new marketplace: open the unfiltered Warehouse Notebooks search and
read the href of its own "Notebooks" department refinement, which carries the correct
`bbn` and node.

## Notes

- Telegram credentials are read from the environment (`TELEGRAM_BOT_TOKEN`,
  `TELEGRAM_CHAT_ID`) and live only in `telegram.env` on the host — never hardcoded in
  `main.py` and never committed to git. If a real token ever does end up in a commit,
  rotating it via @BotFather is the only real fix; removing it from a later commit does
  not remove it from history.
- The search reads only page 1 (24 cards) sorted price-ascending. A page full of
  accessories or unrelated listings can hide real notebooks priced above them.
