"""PineLab keeper - PC-independent, exact-time trigger for the private repo janik4321sdfa/pinelab-gold-signal.

Why (R25, 2026-10-05): GitHub drops or delays the scheduled runs of that repo (30.9. and 1.10. 3h40 late, 2.10. 2 of ~16
crons ran, 5.10. 0 of 12 crons ran; the tsy-monthend-paper crons did not run either). workflow_dispatch starts promptly,
so this keeper waits inside one Actions job and dispatches at exact times. The local Task Scheduler trigger only works
while the PC is on.

The keeper ONLY dispatches workflows; it holds no strategy code, reads no market data and posts no signals. Every target
run is idempotent on the target side (gh_signal: durable signal-ledger + acting window; tsy_monthend: tsy-ledger).

Environment: TARGET_TOKEN (secret; actions:write on the target repo), GH_SELF_TOKEN (this repo's GITHUB_TOKEN),
GITHUB_REPOSITORY, GITHUB_RUN_ID, PARENT_RUN (chain input), DISCORD_WEBHOOK (optional, only for failure notices),
KEEPER_TEST=1 (print the plan and check API access; never dispatches, never sleeps).
Stdlib only.
"""
import os, sys, json, time, datetime as dt, urllib.request, urllib.error
from zoneinfo import ZoneInfo

VERSION = "keeper-1 (R25)"
UTC = dt.timezone.utc
NY = ZoneInfo("America/New_York")
TARGET = "janik4321sdfa/pinelab-gold-signal"
SELF_WF = "keeper.yml"
MAX_RUN = dt.timedelta(minutes=320)        # hand over to a successor well before the 6 h job limit (timeout-minutes 350)
DEDUP = dt.timedelta(minutes=10)           # same workflow dispatched twice within 10 min -> second one skipped
GOLD = {"force": "0", "dry_run": "0"}
TSY = {"dry_run": "0", "now_utc": ""}


def plan(day):
    """Actions of one NY weekday: list of (when_utc, name, workflow, inputs, deadline_utc), sorted.
    gold: send target T = 16:00 NY - 10 min; gh_signal acts only inside -20..+45 min of T (T-42 starts the compute ~T-40).
    gold-deadman: after the acting window gh_signal posts SIGNAL MISSED if nothing visible was recorded (else nothing).
    tsy: same days as its own crons (21-31, 1-7); BUY/SELL announcements are posted before the 16:00 NY close."""
    def at(h, m):
        return dt.datetime(day.year, day.month, day.day, h, m, tzinfo=NY).astimezone(UTC)
    T = at(15, 50)
    acts = []
    if day.day >= 21 or day.day <= 7:
        acts += [(at(10, 15), "tsy-1", "tsy_monthend.yml", TSY, at(15, 0)),
                 (at(14, 15), "tsy-2", "tsy_monthend.yml", TSY, at(15, 30))]
    acts += [(T - dt.timedelta(minutes=42), "gold-1", "signal.yml", GOLD, T + dt.timedelta(minutes=15)),
             (T - dt.timedelta(minutes=24), "gold-2", "signal.yml", GOLD, T + dt.timedelta(minutes=15)),
             (T + dt.timedelta(minutes=35), "gold-deadman", "signal.yml", GOLD, T + dt.timedelta(hours=6))]
    return sorted(acts, key=lambda a: a[0])


def todays_actions(now):
    """Pending actions for the NY date of `now` (weekdays only); actions past their deadline are dropped."""
    day = now.astimezone(NY).date()
    if day.weekday() >= 5:
        return []
    return [a for a in plan(day) if now <= a[4]]


def log(s):
    print(f"{dt.datetime.now(UTC):%Y-%m-%d %H:%M:%SZ} {s}", flush=True)


def api(method, path, token, body=None, tries=4):
    url = "https://api.github.com" + path
    err = None
    for i in range(tries):
        req = urllib.request.Request(url, method=method, data=None if body is None else json.dumps(body).encode(),
                                     headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                                              "User-Agent": "pinelab-keeper", "X-GitHub-Api-Version": "2022-11-28"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                t = r.read().decode()
                return r.status, (json.loads(t) if t else {})
        except urllib.error.HTTPError as e:
            err = f"HTTP {e.code} {e.read()[:200]!r}"
            if e.code not in (408, 429, 500, 502, 503, 504):
                break
        except Exception as e:                     # network
            err = repr(e)
        time.sleep(5 * (i + 1))
    raise RuntimeError(f"{method} {path}: {err}")


def notify(text):
    hook = os.environ.get("DISCORD_WEBHOOK", "")
    if not hook:
        log("no DISCORD_WEBHOOK - notice only in log: " + text); return
    try:
        req = urllib.request.Request(hook + "?wait=true", method="POST", data=json.dumps({"content": text}).encode(),
                                     headers={"Content-Type": "application/json", "User-Agent": "pinelab-keeper"})
        urllib.request.urlopen(req, timeout=30).read()
    except Exception as e:
        log(f"notice failed: {e}")


def sleep_until(t):
    while True:
        left = (t - dt.datetime.now(UTC)).total_seconds()
        if left <= 0:
            return
        time.sleep(min(left, 300))


def other_keeper_wins(self_tok, repo, me, parent):
    """True if another active keeper run (not my chain parent) has a smaller id. Checked twice 45 s apart, so a run
    that is just exiting does not make both runs give up."""
    def rivals():
        ids = set()
        for st in ("in_progress", "queued"):
            _, r = api("GET", f"/repos/{repo}/actions/workflows/{SELF_WF}/runs?status={st}&per_page=30", self_tok)
            ids |= {w["id"] for w in r.get("workflow_runs", [])}
        return {i for i in ids if i < me and str(i) != parent}
    first = rivals()
    if not first:
        return False
    time.sleep(45)
    return bool(first & rivals())


def keepalive(self_tok, repo, now):
    """Public repos lose their schedules after 60 days without activity -> one empty commit when the last is > 25 days old."""
    try:
        _, c = api("GET", f"/repos/{repo}/commits/main", self_tok)
        last = dt.datetime.fromisoformat(c["commit"]["committer"]["date"].replace("Z", "+00:00"))
        if now - last < dt.timedelta(days=25):
            return
        _, nc = api("POST", f"/repos/{repo}/git/commits", self_tok,
                    {"message": f"keepalive {now:%Y-%m-%d}", "tree": c["commit"]["tree"]["sha"], "parents": [c["sha"]]})
        api("PATCH", f"/repos/{repo}/git/refs/heads/main", self_tok, {"sha": nc["sha"], "force": False})
        log(f"keepalive commit {nc['sha'][:7]}")
    except Exception as e:
        log(f"keepalive skipped: {e}")


def main():
    test = os.environ.get("KEEPER_TEST") == "1"
    tok = os.environ.get("TARGET_TOKEN", "")
    self_tok = os.environ.get("GH_SELF_TOKEN", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    me = int(os.environ.get("GITHUB_RUN_ID", "0") or 0)
    parent = os.environ.get("PARENT_RUN", "")
    start = dt.datetime.now(UTC)
    log(f"{VERSION} run {me} parent '{parent}' test={test}")
    acts = todays_actions(start)
    for a in acts:
        log(f"  plan {a[1]:<13} {a[0]:%H:%M}Z ({a[0].astimezone(NY):%H:%M} NY) -> {a[2]} {a[3]} deadline {a[4]:%H:%M}Z")
    if test:
        _, r = api("GET", f"/repos/{TARGET}/actions/workflows", tok)
        log("target workflows: " + ", ".join(f"{w['path']}={w['state']}" for w in r.get("workflows", [])))
        _, r = api("GET", f"/repos/{repo}/actions/workflows/{SELF_WF}/runs?per_page=3", self_tok)
        log(f"self runs visible: {len(r.get('workflow_runs', []))}")
        log("TEST OK (nothing dispatched)"); return 0
    if not acts:
        log("nothing left today"); return 0
    if not tok:
        notify(":warning: **PineLab keeper**: chybi secret TARGET_TOKEN - signal se nespousti z keeperu."); return 2
    if other_keeper_wins(self_tok, repo, me, parent):
        log("another keeper is active - exit"); return 0
    keepalive(self_tok, repo, start)
    last = {}
    for when, name, wf, inputs, deadline in acts:
        if when - start > MAX_RUN:                              # hand over before the job limit
            sleep_until(start + MAX_RUN)
            api("POST", f"/repos/{repo}/actions/workflows/{SELF_WF}/dispatches", self_tok,
                {"ref": "main", "inputs": {"parent": str(me), "test": "0"}})
            log("successor dispatched - exit"); return 0
        sleep_until(when)
        now = dt.datetime.now(UTC)
        if now > deadline:
            log(f"{name}: past deadline - skipped"); continue
        if name != "gold-deadman" and wf in last and now - last[wf] < DEDUP:
            log(f"{name}: {wf} dispatched {(now - last[wf]).seconds // 60} min ago - skipped"); continue
        try:
            api("POST", f"/repos/{TARGET}/actions/workflows/{wf}/dispatches", tok, {"ref": "main", "inputs": inputs})
            last[wf] = now
            log(f"{name}: dispatched {wf} {inputs}")
        except Exception as e:
            log(f"{name}: DISPATCH FAILED {e}")
            notify(f":warning: **PineLab keeper**: spusteni `{wf}` ({name}) selhalo: {str(e)[:150]}. "
                   f"Signal muze chybet - zkontroluj GitHub token (secret TARGET_TOKEN).")
    log("day done"); return 0


if __name__ == "__main__":
    sys.exit(main())
