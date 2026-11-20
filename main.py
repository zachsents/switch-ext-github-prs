"""github-prs: pull requests in the user's active GitHub repos, as entities, so work can wake when one merges.

An appendage: an MCP server its keeper runs and hosts as github-prs@<machine>. Its tools are its
actions, and what it emits lands in the brain where each event type says (/docs/appendages).

It finds the repos where the user's GitHub account authored a pull request in the last two weeks
(and any repo a `pr` call names), and polls each one's pull requests and Actions runs with ETags, so an
unchanged answer (a 304) costs nothing against the rate limit: every 10 s while a repo is busy, every
2 min while it's quiet. Each pull request is the entity `pr:<owner>/<repo>#<number>` (lowercase), and it
records an observation in github/prs only when something that matters changes: opened, pushed, retitled,
draft or ready, merged, closed, reopened, or its checks (the latest Actions run of each workflow on its
head commit) start, pass, fail or are cancelled. Merge trains (scripts/train.sh) are followed: which
open train carries each pull request now, what replaced a closed train, and every pull request a
train landed shows merged, whatever GitHub shows for it.

- Its tests: python3 -m unittest -q
- What its keeper will find in it: sb probe -- python3 main.py
- Installing it: sb install github-prs --dir . --on macos@local --test 'python3 -m unittest -q'
  --try status --wait --health-every 600000 -- python3 main.py
"""

import calendar
import hashlib
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import subprocess

import sbmcp

NAME = "github-prs"
HOME = os.environ.get("SB_HOME") or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".home")
"""Where it keeps what lasts between runs: its keeper's SB_HOME, or .home by hand."""
API = os.environ.get("GITHUB_PRS_API") or "https://api.github.com"
HEARTBEAT_S = float(os.environ.get("GITHUB_PRS_HEARTBEAT_S") or 300)
FIRST_BEAT_S = float(os.environ.get("GITHUB_PRS_FIRST_BEAT_S") or 15)
"""Its first heartbeat waits this long, for its first look at GitHub, so it says how that went."""
"""How often it says how it is, in vitals/github-prs (--health-every 600000 promises at least every 10 min)."""
BUSY_S = float(os.environ.get("GITHUB_PRS_BUSY_S") or 10)
"""How often a busy repo is polled: one with a pull request changed in the last 2 hours, or checks running."""
QUIET_S = float(os.environ.get("GITHUB_PRS_QUIET_S") or 120)
"""How often a quiet repo is polled."""
BUSY_FOR_S = 2 * 3600
DISCOVER_S = float(os.environ.get("GITHUB_PRS_DISCOVER_S") or 600)
"""How often it looks again for the repos to watch."""
WINDOW_DAYS = 14
"""A repo is watched while the user's account authored a pull request there within this many days."""
ASKED_FOR_S = 3 * 86400
"""A repo a `pr` call named is watched this long after, whoever's pull requests it has."""
KEEP_DAYS = 30
"""What it remembers of a closed pull request goes once it's this old."""
EXTRA = [r.strip() for r in (os.environ.get("GITHUB_PRS_REPOS") or "").split(",") if r.strip()]
"""Repos always watched, besides the ones it finds (owner/repo, comma-separated)."""
DEFAULT_REPO = os.environ.get("GITHUB_PRS_DEFAULT_REPO") or ""
PAGE = 50
FAILING = {"failure", "timed_out", "startup_failure", "action_required"}
FINE = {"success", "skipped", "neutral"}

INSTRUCTIONS = """\
Pull requests as entities work wakes on, sensed within ~10 s: pr:<owner>/<repo>#<number> (lowercase, like pr:owner/repo#371), facts.state open|merged|closed, facts.checks running|passed|failed|cancelled. To act the moment one merges or closes, the brain tool's expect with when `entities | where id == "pr:owner/repo#371" and facts.state != "open"`; for its CI, `... and facts.head_sha == "<sha>" and facts.checks in ("passed", "failed", "cancelled")` (failed as soon as one workflow fails). pr(number, repo) reads one fresh and answers with those queries filled in; prs lists what's sensed; status says how the sense is.

Merge trains (scripts/train.sh: a train/ branch merging several pull requests onto main, landed by main fast-forwarding to it) are followed through every train that replaces another. An open pull request a train carries at its newest commit has facts.train, that train's number. Once a train lands, each one it carried at its newest commit is merged, with facts.landed_by naming the train, whatever GitHub shows for it (a rider closed by hand, or not marked yet); so is any pull request closed unmerged whose newest commit main already holds. So to act once a pull request is on main, wait on its own merged query, whichever train ends up landing it, never on a train, which may be replaced. A train itself has facts.carries (the numbers it carries, in order) and carried_heads (each one's commit, short), and once it closes unmerged, facts.replaced_by (the train that took its place).

Each observation (in github/prs) carries repo, number, ref (switchboard#371), title, state, draft, author, head_ref, head_sha, base_ref, url, created_at, updated_at, merged_at, closed_at, merge_commit, checks with checks_runs (the latest Actions run of each workflow on its head commit), train, landed_by, carries, carried_heads, replaced_by, and changed (seen, opened, pushed, retitled, draft, ready, merged, closed, reopened, checks …, on train #N, off its train, replaced by #N). It's recorded only when one of those changes, never twice for the same state.

It watches every repo where the user's account authored a pull request in the last two weeks (looked for every 10 min), and any repo a pr call names for three days after, polling each with ETags (free when nothing changed): every 10 s while it's busy, every 2 min while it's quiet."""

app = sbmcp.Server(NAME, instructions=INSTRUCTIONS, requests=[], notifies=False, reads=[])
app.event("pr", "A pull request opened, changed, merged or closed, or its checks moved", stream="github/prs", kind="observation", retention="episode")
app.event("heartbeat", "How the sense is itself", stream=f"vitals/{NAME}", kind="observation", retention="signal")

lock = threading.RLock()
stats = {"started_at": int(time.time() * 1000), "polls": 0, "requests": 0, "not_modified": 0, "observed": 0,
         "rate_remaining": None, "rate_reset": None, "last_poll_at": None, "last_change_at": None, "error": None,
         "last_failure": None, "last_failure_at": None, "login": None}
repos = {}   # full_name -> {"next": epoch s, "busy_until": epoch s, "polled_at", "error", "max_updated": iso}
known = {}   # entity -> facts as last recorded
asked = {}   # full_name -> until (epoch s)
landed = {}  # entity -> {"head", "by", "at", "commit"}: on its base though GitHub may not say merged
looked = set()  # trains closed unmerged whose replacement it has looked for since it started
discovered = {"at": 0.0, "next": 0.0, "repos": []}


def now():
    return time.time()


def ms(iso):
    if not iso:
        return None
    return calendar.timegm(time.strptime(iso, "%Y-%m-%dT%H:%M:%SZ")) * 1000


# ---------------------------------------------------------------------------------------------
# GitHub, reached through one function

def fetch(url, headers):
    """The world outside: one HTTP GET to GitHub, as (status, headers in lowercase, body bytes). Its
    tests put a fake here."""
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, e.read() or b""


def token_from_gh():
    try:
        p = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=20)
        return p.stdout.strip() if p.returncode == 0 else ""
    except Exception:
        return ""


class GitHub:
    """GitHub's REST API as the user's gh login, ETag-cached: an answer that hasn't changed (a 304) costs
    nothing against the rate limit, and says so (changed False)."""

    def __init__(self):
        self.token = None
        self.cache = {}  # url -> (etag, parsed)
        self.lock = threading.Lock()

    def _token(self, again=False):
        if again or not self.token:
            env = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
            self.token = (token_from_gh() or env) if again else (env or token_from_gh())
            if not self.token:
                raise RuntimeError("no GitHub token: gh auth token gave nothing (gh auth login on this Mac)")
        return self.token

    def get(self, path, **params):
        """(parsed, changed): what GitHub answers, and whether it's new since the last time this was asked."""
        url = f"{API}/{path.lstrip('/')}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        for attempt in (1, 2):
            headers = {"Authorization": f"Bearer {self._token(again=attempt == 2)}", "Accept": "application/vnd.github+json",
                       "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "switchboard-github-prs"}
            with self.lock:
                cached = self.cache.get(url)
            if cached:
                headers["If-None-Match"] = cached[0]
            status, hdrs, raw = fetch(url, headers)
            with lock:
                stats["requests"] += 1
                if hdrs.get("x-ratelimit-remaining"):
                    stats["rate_remaining"] = int(hdrs["x-ratelimit-remaining"])
                    stats["rate_reset"] = int(hdrs.get("x-ratelimit-reset") or 0) * 1000 or None
            if status == 401 and attempt == 1:
                continue
            if status == 304 and cached:
                with lock:
                    stats["not_modified"] += 1
                return cached[1], False
            if status == 404:
                raise LookupError(f"GitHub has no {path} (or this login can't see it)")
            if status >= 400:
                raise RuntimeError(f"GitHub GET {path} answered {status}: {raw.decode(errors='replace')[:200]}")
            parsed = json.loads(raw) if raw else {}
            if hdrs.get("etag"):
                with self.lock:
                    self.cache[url] = (hdrs["etag"], parsed)
                    if len(self.cache) > 300:
                        for k in list(self.cache)[:100]:
                            del self.cache[k]
            return parsed, True
        raise RuntimeError("GitHub: not signed in (gh auth login on this Mac)")


gh = GitHub()


# ---------------------------------------------------------------------------------------------
# What a pull request is, and what changed

def entity_of(repo, number):
    return f"pr:{repo.lower()}#{int(number)}"


def facts_of(repo, p, prior=None):
    """A pull request as GitHub lists it, as the facts its entity keeps (its checks carried over while
    its head commit is the same)."""
    prior = prior or {}
    state = "merged" if p.get("merged_at") else p.get("state")
    head = p.get("head") or {}
    f = {
        "repo": repo, "number": p["number"], "ref": f"{repo.split('/')[-1]}#{p['number']}", "title": p.get("title"),
        "state": state, "draft": bool(p.get("draft")), "author": (p.get("user") or {}).get("login"),
        "head_ref": head.get("ref"), "head_sha": head.get("sha"), "base_ref": (p.get("base") or {}).get("ref"),
        "url": p.get("html_url"), "created_at": ms(p.get("created_at")), "updated_at": ms(p.get("updated_at")),
        "merged_at": ms(p.get("merged_at")), "closed_at": ms(p.get("closed_at")),
        "merge_commit": p.get("merge_commit_sha") if state == "merged" else None,
        "checks": None, "checks_runs": None,
    }
    if prior.get("head_sha") == f["head_sha"]:
        f["checks"], f["checks_runs"] = prior.get("checks"), prior.get("checks_runs")
    rows = carried(p.get("body")) if is_train(f) else None
    f["carries"] = [n for n, _ in rows] if rows is not None else None
    f["carried_heads"] = {str(n): h for n, h in rows} if rows is not None else None
    f["replaced_by"] = prior.get("replaced_by") if f["state"] == "closed" else None
    # Which train carries it now is the repo's to say (follow_trains); until then, what it was.
    f["train"] = prior.get("train") if f["state"] == "open" and prior.get("head_sha") == f["head_sha"] else None
    return overlay(entity_of(repo, p["number"]), f)


def overlay(e, f):
    """f with what GitHub may not show: merged once main holds its newest commit (a train it rode landed,
    or its commit reached main another way), with the train that landed it."""
    with lock:
        got = landed.get(e)
    f = {**f, "landed_by": None}
    if not got or not f.get("head_sha") or not f["head_sha"].startswith(got["head"]):
        return f
    f["landed_by"] = got.get("by")
    if f["state"] != "merged":
        f["state"] = "merged"
        f["merged_at"] = f.get("merged_at") or got.get("at")
        f["merge_commit"] = got.get("commit")
    return f


# ---------------------------------------------------------------------------------------------
# Merge trains: what each carries, what replaced it, and what landed with it

TRAIN_LINE = re.compile(r"^- #(\d+) \(([0-9a-f]{7,40})\)", re.M)
REPLACED = re.compile(r"\bReplaced by #(\d+)")


def is_train(f):
    return str(f.get("head_ref") or "").startswith("train/")


def carried(body):
    """What a train says it carries, in order, as (number, its commit as carried, short): scripts/train.sh
    writes a line `- #N (abcd1234): title` for each, and none for what it set aside."""
    return [(int(n), h) for n, h in TRAIN_LINE.findall(body or "")]


def land(repo, number, head, by, at, commit):
    """Notes that main holds number's commit head (landed by train by, or None), so it shows merged."""
    e = entity_of(repo, number)
    with lock:
        landed[e] = {"head": head, "by": by, "at": at, "commit": commit, "noted": int(now() * 1000)}
    return e


def replacement(repo, number):
    """The train that took a closed train's place, from scripts/train.sh's "Replaced by #N." comment."""
    try:
        comments, _ = gh.get(f"repos/{repo}/issues/{int(number)}/comments", per_page=100)
    except Exception as e:
        failed(f"reading #{number}'s comments", e)
        return None
    for c in reversed(comments or []):
        m = REPLACED.search(c.get("body") or "")
        if m:
            return int(m.group(1))
    return None


def holds(repo, base, head):
    """Whether base (a branch) holds commit head already: GitHub's compare says head is behind it or the same."""
    try:
        got, _ = gh.get(f"repos/{repo}/compare/{urllib.parse.quote(base, safe='')}...{head}", per_page=1)
    except Exception as e:
        failed(f"comparing {head[:8]} with {base}", e)
        return False
    return got.get("status") in ("behind", "identical")


def relanded(fresh, entities):
    """Puts in fresh, as they now show, the pull requests just noted as landed."""
    for e in entities:
        with lock:
            f = fresh.get(e) or known.get(e)
        if f:
            fresh[e] = overlay(e, f)


def follow_trains(repo, fresh):
    """What merge trains say of the pull requests in fresh (entity -> facts as GitHub has them now) and of
    the rest of repo's, as more of fresh: a train that landed lands what it carried at their newest
    commits; one closed unmerged names what replaced it; one closed unmerged that isn't a train is
    merged if main holds its commit; and each open one carries the number of the newest open train
    carrying it at its newest commit (so it follows a train that replaces another)."""
    with lock:
        before = {e: known.get(e) for e in fresh}
    touched = set()
    for e, f in list(fresh.items()):
        was = (before.get(e) or {}).get("state")
        if is_train(f) and f["state"] == "merged" and was != "merged":
            for n in f.get("carries") or []:
                touched.add(land(repo, n, f["carried_heads"][str(n)], f["number"], f.get("merged_at"), f.get("merge_commit") or f["head_sha"]))
        elif is_train(f) and f["state"] == "closed" and not f.get("replaced_by") and e not in looked and (
                was != "closed" or (f.get("closed_at") or 0) > (now() - 86400) * 1000):
            # Once a run, so one closed before it started (or before it read comments) is named too.
            looked.add(e)
            fresh[e] = {**f, "replaced_by": replacement(repo, f["number"])}
    relanded(fresh, touched)
    with lock:
        merged_trains = [g for g in list(known.values()) + list(fresh.values()) if g["repo"] == repo and is_train(g) and g["state"] == "merged"]
    touched = set()
    for e, f in list(fresh.items()):
        was = (before.get(e) or {}).get("state")
        if f["state"] == "closed" and was != "closed" and not is_train(f) and f.get("head_sha") and f.get("base_ref"):
            if holds(repo, f["base_ref"], f["head_sha"]):
                by = next((g["number"] for g in merged_trains if f["head_sha"].startswith((g.get("carried_heads") or {}).get(str(f["number"])) or "-")), None)
                touched.add(land(repo, f["number"], f["head_sha"], by, f.get("closed_at"), f["head_sha"]))
    relanded(fresh, touched)
    # Which open train carries each open pull request at its newest commit: the newest, when two do
    # for a moment as one replaces the other.
    with lock:
        every = {**{e: f for e, f in known.items() if f["repo"] == repo}, **fresh}
    on = {}
    for f in every.values():
        if is_train(f) and f["state"] == "open":
            for n, h in (f.get("carried_heads") or {}).items():
                if int(n) not in on or on[int(n)][0] < f["number"]:
                    on[int(n)] = (f["number"], h)
    for e, f in every.items():
        if f["state"] != "open" or is_train(f):
            continue
        t, h = on.get(f["number"], (None, None))
        want = t if t and (f.get("head_sha") or "").startswith(h) else None
        if want != f.get("train"):
            fresh[e] = {**f, "train": want}
    return fresh


MATTERS = ("state", "draft", "head_sha", "title", "base_ref", "checks", "train", "landed_by", "replaced_by")


def what_changed(prior, f, watched_since=None):
    """The words for what changed: [] when nothing that matters did. A pull request it hasn't seen is
    "opened" (or "merged", "closed") when it's newer than its repo's watch, else "seen" as it stands."""
    if not prior:
        if watched_since is None or (f.get("created_at") or 0) < watched_since:
            return ["seen"]
        return [{"merged": "merged", "closed": "closed"}.get(f["state"], "opened")]
    out = []
    if prior.get("state") != f["state"]:
        out.append({"merged": "merged", "closed": "closed"}.get(f["state"], "reopened"))
    if prior.get("head_sha") != f["head_sha"] and f["state"] == "open":
        out.append("pushed")
    if prior.get("title") != f["title"]:
        out.append("retitled")
    if prior.get("draft") != f["draft"]:
        out.append("draft" if f["draft"] else "ready")
    if prior.get("base_ref") != f["base_ref"]:
        out.append("rebased onto " + str(f["base_ref"]))
    if prior.get("checks") != f["checks"] and f["checks"]:
        out.append("checks " + f["checks"])
    if prior.get("train") != f.get("train") and f["state"] == "open":
        out.append(f"on train #{f['train']}" if f.get("train") else "off its train")
    if prior.get("replaced_by") != f.get("replaced_by") and f.get("replaced_by"):
        out.append(f"replaced by #{f['replaced_by']}")
    if prior.get("landed_by") != f.get("landed_by") and f.get("landed_by") and "merged" not in out:
        out.append(f"landed with train #{f['landed_by']}")
    return out


def summary_of(f, changed):
    words = {"seen": {"merged": "is merged", "closed": "is closed"}.get(f["state"], "is open"), "opened": "opened", "merged": "merged", "closed": "closed without merging", "reopened": "reopened",
             "pushed": "has a new commit " + (f["head_sha"] or "")[:7], "retitled": "retitled", "draft": "back to draft",
             "ready": "ready for review", "checks running": "checks running", "checks passed": "checks passed",
             "checks failed": "checks failed", "checks cancelled": "checks cancelled"}
    if f.get("landed_by"):
        words["merged"] = f"merged with train #{f['landed_by']}"
    if f.get("replaced_by"):
        words["closed"] = "closed"
    said = ", ".join(words.get(c, c) for c in changed)
    if f["state"] == "open" and f["checks"] and not any(c.startswith("checks") for c in changed) and "pushed" not in changed:
        said += f" (checks {f['checks']})"
    return f"{f['ref']} {said}: {f['title']}"


def observe(f, changed):
    """Records the pull request's state as an observation of its entity. Its id is the state's own, so
    the same state sent twice (a restart) is recorded once."""
    e = entity_of(f["repo"], f["number"])
    fingerprint = json.dumps([e] + [f.get(k) for k in MATTERS] + [f.get("updated_at"), f.get("checks_runs")], sort_keys=True)
    rid = "pr-" + hashlib.sha256(fingerprint.encode()).hexdigest()[:40]
    data = {"entity": e, "kind": "pull-request", "name": f"{f['ref']} {f['title']}"[:200], "summary": summary_of(f, changed),
            "facts": {**f, "changed": changed}}
    author = f.get("author") or ""
    sender = {"handle": author.lower(), "via": "github"} if author else None
    if sender and re.search(r"\[bot\]$|^dependabot|^renovate|^github-actions", author, re.I):
        sender["agent"] = True
    app.emit("pr", data, about=[e], id=rid, event_id=rid, importance=0.5 if f["state"] != "open" else 0.2, sender=sender)
    with lock:
        known[e] = f
        stats["observed"] += 1
        stats["last_change_at"] = int(now() * 1000)
    return data


def consider(f):
    """Records f when what matters about it changed since it was last recorded; the record, else None."""
    e = entity_of(f["repo"], f["number"])
    with lock:
        prior = known.get(e)
        since = (repos.get(f["repo"]) or {}).get("watched_since")
    changed = what_changed(prior, f, since)
    if not changed:
        with lock:
            known[e] = f  # keep its times fresh without a record
        return None
    return observe(f, changed)


def rollup(runs):
    """The checks on one commit, from its Actions runs: the latest run of each workflow, as failed (any
    failed, even while others run), running, passed (all fine) or cancelled, with the runs."""
    latest = {}
    for r in runs:
        key = r.get("workflow_id") or r.get("name")
        if key not in latest or (r.get("created_at") or "", r.get("id") or 0) > (latest[key].get("created_at") or "", latest[key].get("id") or 0):
            latest[key] = r
    if not latest:
        return None, None
    rows = sorted(({"workflow": r.get("name"), "status": r.get("status"), "conclusion": r.get("conclusion"), "run": r.get("id"),
                    "attempt": r.get("run_attempt"), "url": r.get("html_url")} for r in latest.values()), key=lambda x: str(x["workflow"]))
    if any(r.get("status") == "completed" and r.get("conclusion") in FAILING for r in latest.values()):
        verdict = "failed"  # as GitHub's own rollup: one failure fails it, whatever else still runs
    elif any(r.get("status") != "completed" for r in latest.values()):
        verdict = "running"
    elif all(r.get("conclusion") in FINE for r in latest.values()):
        verdict = "passed"
    else:
        verdict = "cancelled"
    return verdict, rows


# ---------------------------------------------------------------------------------------------
# Polling

def pulls_since(repo, prior_max):
    """Its pull requests newest-changed first: the first page, and more pages while every one on the
    page changed since it last looked (a merge train merging dozens at once). None when nothing changed."""
    page, changed = gh.get(f"repos/{repo}/pulls", state="all", sort="updated", direction="desc", per_page=PAGE)
    if not changed:
        return None
    got = list(page)
    n = 1
    while prior_max and len(page) == PAGE and (page[-1].get("updated_at") or "") > prior_max and n < 6:
        n += 1
        page, _ = gh.get(f"repos/{repo}/pulls", state="all", sort="updated", direction="desc", per_page=PAGE, page=n)
        got += page
    return got


def entry(repo):
    """What it keeps of a repo it watches (made the first time), under the lock."""
    return repos.setdefault(repo, {"next": 0, "busy_until": 0, "polled_at": None, "error": None, "max_updated": None, "watched_since": None})


def runs_of(repo, **params):
    """Its latest Actions runs (params narrow them), and whether they changed: (None, False) for a repo
    without Actions, which it then asks again only hourly."""
    with lock:
        if (repos.get(repo) or {}).get("no_actions_until", 0) > now():
            return None, False
    try:
        return gh.get(f"repos/{repo}/actions/runs", per_page=PAGE, **params)
    except LookupError:
        with lock:
            entry(repo)["no_actions_until"] = now() + 3600
        return None, False


def poll(repo):
    """One look at a repo: its pull requests and Actions runs, recording what changed. How many records."""
    with lock:
        r = entry(repo)
        prior_max = r.get("max_updated")
    out = 0
    pulls = pulls_since(repo, prior_max)
    runs, runs_changed = runs_of(repo)
    t = now()
    fresh = {}
    if pulls is not None:
        for p in pulls:
            e = entity_of(repo, p["number"])
            with lock:
                prior = known.get(e)
            if not prior and p.get("state") != "open" and (ms(p.get("updated_at")) or 0) < (t - 86400) * 1000:
                continue  # long settled before it watched: nothing to wake anyone for
            fresh[e] = facts_of(repo, p, prior)
        top = max((p.get("updated_at") or "" for p in pulls), default=None)
        with lock:
            if top and (not r["max_updated"] or top > r["max_updated"]):
                r["max_updated"] = top
    # Checks: the runs on each open pull request's head commit.
    by_sha = {}
    for run in (runs or {}).get("workflow_runs", []):
        by_sha.setdefault(run.get("head_sha"), []).append(run)
    running = any(run.get("status") != "completed" for run in (runs or {}).get("workflow_runs", []))
    if runs_changed or fresh:
        with lock:
            candidates = {e: f for e, f in known.items() if f["repo"] == repo and f["state"] == "open"}
        candidates.update({e: f for e, f in fresh.items() if f["state"] == "open"})
        for e, f in candidates.items():
            verdict, rows = rollup(by_sha.get(f["head_sha"], []))
            if verdict and (verdict != f.get("checks") or rows != f.get("checks_runs")):
                fresh[e] = {**fresh.get(e, f), "checks": verdict, "checks_runs": rows}
    fresh = follow_trains(repo, fresh)
    for e, f in fresh.items():
        if consider(f):
            out += 1
    with lock:
        busy = any((f.get("updated_at") or 0) > (t - BUSY_FOR_S) * 1000 for f in known.values() if f["repo"] == repo)
        if busy or running:
            r["busy_until"] = t + (BUSY_FOR_S if busy else 600)
        r["polled_at"] = int(t * 1000)
        r["watched_since"] = r.get("watched_since") or int(t * 1000)
        r["error"] = None
        r["next"] = t + (BUSY_S if t < r["busy_until"] else QUIET_S)
        stats["polls"] += 1
        stats["last_poll_at"] = int(t * 1000)
    return out


def discover(force=False):
    """The repos to watch: where the user's account authored a pull request lately, the extra ones, and the
    ones a pr call named."""
    t = now()
    with lock:
        if not force and t < discovered["next"]:
            return list(repos)
        discovered["next"] = t + 60  # a failed look tries again in a minute
    since = time.strftime("%Y-%m-%d", time.gmtime(t - WINDOW_DAYS * 86400))
    found, _ = gh.get("search/issues", q=f"is:pr author:@me updated:>={since}", per_page=100, sort="updated")
    names = {i["repository_url"].split("/repos/", 1)[1] for i in found.get("items", []) if i.get("repository_url")}
    with lock:
        if not stats["login"]:
            try:
                stats["login"] = gh.get("user")[0].get("login")
            except Exception:
                pass
        for repo, until in list(asked.items()):
            if until < t:
                del asked[repo]
        names |= set(EXTRA) | set(asked)
        for repo in list(repos):
            if repo not in names:
                del repos[repo]
        for repo in names:
            entry(repo)
        discovered["at"], discovered["next"], discovered["repos"] = t, t + DISCOVER_S, sorted(names)
    return sorted(names)


def due():
    t = now()
    with lock:
        return sorted((r["next"], name) for name, r in repos.items() if r["next"] <= t)


def failed(where, e):
    with lock:
        stats["error"] = stats["last_failure"] = f"{where}: {type(e).__name__}: {e}"[:400]
        stats["last_failure_at"] = int(now() * 1000)
    app.log(stats["error"])


def tick():
    """One pass of the loop: find the repos (now and then), poll the ones that are due."""
    ok = True
    try:
        discover()
    except Exception as e:
        ok = False
        failed("finding repos", e)
    for _, repo in due():
        try:
            poll(repo)
        except Exception as e:
            ok = False
            failed(repo, e)
            with lock:
                if repo in repos:
                    repos[repo]["error"] = str(e)[:300]
                    repos[repo]["next"] = now() + QUIET_S
    if ok:
        with lock:
            stats["error"] = None  # a clean pass: what failed before has come right (last_failure keeps it)
    save()


# ---------------------------------------------------------------------------------------------
# What lasts between runs

_saved = {"digest": None}


def save():
    t = now()
    with lock:
        for e, f in list(known.items()):
            if f["state"] != "open" and (f.get("updated_at") or 0) < (t - KEEP_DAYS * 86400) * 1000:
                del known[e]
        for e, got in list(landed.items()):
            if (got.get("noted") or 0) < (t - KEEP_DAYS * 86400) * 1000:
                del landed[e]
        body = json.dumps({"known": known, "asked": asked, "landed": landed, "repos": {k: {"max_updated": v.get("max_updated"), "watched_since": v.get("watched_since")}
                                                                       for k, v in repos.items()}}, sort_keys=True)
    digest = hashlib.sha256(body.encode()).hexdigest()
    if digest == _saved["digest"]:
        return
    path = os.path.join(HOME, "state.json")
    os.makedirs(HOME, mode=0o700, exist_ok=True)
    with open(path + ".tmp", "w") as fh:
        fh.write(body)
    os.replace(path + ".tmp", path)
    _saved["digest"] = digest


def load():
    try:
        with open(os.path.join(HOME, "state.json")) as fh:
            saved = json.load(fh)
    except (OSError, ValueError):
        return
    with lock:
        known.update(saved.get("known") or {})
        asked.update(saved.get("asked") or {})
        landed.update(saved.get("landed") or {})
        for repo, kept in (saved.get("repos") or {}).items():
            entry(repo).update({k: kept.get(k) for k in ("max_updated", "watched_since")})


# ---------------------------------------------------------------------------------------------
# Tools

def waits_for(e, f):
    out = {"done": f'entities | where id == "{e}" and facts.state != "open"',
           "merged": f'entities | where id == "{e}" and facts.state == "merged"'}
    if f.get("state") == "open" and f.get("head_sha"):
        out["checks"] = f'entities | where id == "{e}" and facts.head_sha == "{f["head_sha"]}" and facts.checks in ("passed", "failed", "cancelled")'
    return out


@app.tool(read_only=True)
def pr(number: int, repo: str = DEFAULT_REPO):
    """One pull request as GitHub has it now (asked fresh, and recorded if it changed), with its entity and the `when` queries that wake work once it's merged or closed (done), merged, or its checks on this head finish (checks). repo is owner/name (default owner/repo); naming one watches it for three days whoever's pull requests it has."""
    repo = repo.strip().strip("/")
    if repo.count("/") != 1:
        raise ValueError(f"repo is owner/name, like owner/repo, not {repo!r}")
    e = entity_of(repo, number)
    p, _ = gh.get(f"repos/{repo}/pulls/{int(number)}")
    full = p.get("base", {}).get("repo", {}).get("full_name") or repo
    with lock:
        prior = known.get(e)
        asked[full] = now() + ASKED_FOR_S
        r = entry(full)
        r["busy_until"] = max(r["busy_until"], now() + BUSY_FOR_S)
        r["next"] = min(r["next"], now() + BUSY_S)
    f = facts_of(full, p, prior)
    if f["state"] == "open" and f["head_sha"]:
        runs, _ = runs_of(full, head_sha=f["head_sha"])
        verdict, rows = rollup((runs or {}).get("workflow_runs", []))
        if verdict:
            f["checks"], f["checks_runs"] = verdict, rows
    more = follow_trains(full, {e: f})
    f = more.pop(e)
    recorded = consider(f)
    for g in more.values():
        consider(g)
    save()
    return {"entity": e, **f, "recorded": bool(recorded), "when": waits_for(e, f)}


@app.tool(read_only=True)
def prs(repo: str = None, state: str = None, limit: int = 30):
    """The pull requests it has sensed, the most recently changed first: repo (owner/name) and state (open, merged, closed) narrow them."""
    with lock:
        rows = [{"entity": e, **{k: f.get(k) for k in ("ref", "title", "state", "draft", "checks", "head_sha", "train", "landed_by", "replaced_by", "updated_at", "merged_at", "url")}}
                for e, f in known.items()
                if (not repo or f["repo"].lower() == repo.lower()) and (not state or f["state"] == state)]
    rows.sort(key=lambda x: x.get("updated_at") or 0, reverse=True)
    return {"count": len(rows), "prs": rows[:max(1, min(int(limit or 30), 200))]}


@app.tool(read_only=True)
def status():
    """How the sense is: the repos it watches (each polled when, how often, its last error), its requests to GitHub and how many came back unchanged and free, the rate limit left, and its last error."""
    t = now()
    with lock:
        watched = [{"repo": name, "polled_at": r.get("polled_at"), "every_s": BUSY_S if t < r["busy_until"] else QUIET_S,
                    "open": sum(1 for f in known.values() if f["repo"] == name and f["state"] == "open"),
                    "error": r.get("error"), "asked": name in asked} for name, r in sorted(repos.items())]
        return {**stats, "repos": watched, "known": len(known), "discovered_at": int(discovered["at"] * 1000) or None}


def heartbeat():
    """Says how it is, in vitals/github-prs: what keeps its --health-every promise."""
    with lock:
        n = len(repos)
        busy = sum(1 for r in repos.values() if now() < r["busy_until"])
        facts = {**stats, "repos": n, "busy_repos": busy, "known": len(known)}
        error = stats["error"]
    said = f"Watching {n} repos ({busy} busy), {len(known)} pull requests known"
    app.emit("heartbeat", {"entity": f"sense:{NAME}", "summary": f"Failing: {error}" if error else said, "facts": facts})


@app.background
def watch():
    load()
    while True:
        tick()
        time.sleep(1)


@app.background
def beat():
    time.sleep(FIRST_BEAT_S)
    while True:
        heartbeat()
        time.sleep(HEARTBEAT_S)


if __name__ == "__main__":
    os.makedirs(HOME, mode=0o700, exist_ok=True)
    app.run()
