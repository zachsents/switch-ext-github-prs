// Polling: it finds the repos where the user's GitHub account authored a pull request in the last two
// weeks (and any repo a `pr` call names), and polls each one's pull requests and Actions runs with
// ETags, so an unchanged answer (a 304) costs nothing against the rate limit: every 10 s while a repo
// is busy, every 2 min while it's quiet. What lasts between runs is in home/state.json.
import { existsSync, mkdirSync, readFileSync } from "node:fs"
import path from "node:path"
import { Cause, Effect, Option, Schema } from "effect"
import { writeFileAtomic } from "switch-ext-kit"

import { env, Found, getAs, LookupError, Pulls, Runs, stats, User } from "./github.ts"
import type { Pull } from "./github.ts"
import {
  asked,
  consider,
  Emit,
  entityOf,
  entry,
  Facts,
  factsOf,
  failed,
  filled,
  followTrains,
  Heartbeat,
  known,
  Landed,
  landed,
  log,
  ms,
  NAME,
  now,
  repos,
  rollup,
  same,
} from "./prs.ts"

const seconds = (name: string, fallback: number) => Number(env(name) ?? fallback)

/** How often it says how it is, in vitals/github-prs. */
const HEARTBEAT_S = seconds("GITHUB_PRS_HEARTBEAT_S", 300)
/** Its first heartbeat waits this long, for its first look at GitHub, so it says how that went. */
const FIRST_BEAT_S = seconds("GITHUB_PRS_FIRST_BEAT_S", 15)
/** How often a busy repo is polled: one with a pull request changed in the last 2 hours, or checks running. */
export const BUSY_S = seconds("GITHUB_PRS_BUSY_S", 10)
/** How often a quiet repo is polled. */
export const QUIET_S = seconds("GITHUB_PRS_QUIET_S", 120)
export const BUSY_FOR_S = 2 * 3600
/** How often it looks again for the repos to watch. */
const DISCOVER_S = seconds("GITHUB_PRS_DISCOVER_S", 600)
/** A repo is watched while the user's account authored a pull request there within this many days. */
const WINDOW_DAYS = 14
/** What it remembers of a closed pull request goes once it's this old. */
const KEEP_DAYS = 30
/** Repos always watched, besides the ones it finds (owner/repo, comma-separated). */
const EXTRA = (env("GITHUB_PRS_REPOS") ?? "")
  .split(",")
  .map((r) => r.trim())
  .filter(Boolean)
const PAGE = 50
const MAX_PAGES = 6
const DAY_S = 86_400
const HOUR_S = 3600
const RETRY_S = 60
const RUNNING_FOR_S = 600
const ERROR_LENGTH = 300
const REPOS = "/repos/"

export const HOME = path.resolve("home")
const STATE = path.join(HOME, "state.json")

export const discovered: { at: number; next: number; repos: readonly string[] } = {
  at: 0,
  next: 0,
  repos: [],
}

/**
 * Its pull requests newest-changed first: the first page, and more pages while every one on the page
 * changed since it last looked (a merge train merging dozens at once). null when nothing changed.
 */
const pullsSince = (repo: string, priorMax: string | null) =>
  Effect.gen(function* paging() {
    const asking = { state: "all", sort: "updated", direction: "desc", per_page: PAGE }
    const first = yield* getAs(Pulls, `repos/${repo}/pulls`, asking)
    if (!first.changed) {
      return null
    }
    let page = first.parsed
    const got: Pull[] = [...page]
    if (!filled(priorMax)) {
      return got
    }
    let n = 1
    while (page.length === PAGE && (page.at(-1)?.updated_at ?? "") > priorMax && n < MAX_PAGES) {
      n += 1
      page = (yield* getAs(Pulls, `repos/${repo}/pulls`, { ...asking, page: n })).parsed
      got.push(...page)
    }
    return got
  })

/**
 * Its latest Actions runs (params narrow them), and whether they changed: null for a repo without
 * Actions, which it then asks again only hourly.
 */
export const runsOf = (repo: string, params: Readonly<Record<string, string>> = {}) =>
  Effect.gen(function* running() {
    if ((repos.get(repo)?.no_actions_until ?? 0) > now()) {
      return { runs: null, changed: false }
    }
    return yield* getAs(Runs, `repos/${repo}/actions/runs`, { per_page: PAGE, ...params }).pipe(
      Effect.map(({ parsed, changed }) => ({ runs: parsed, changed })),
      Effect.catchIf(
        (e) => e instanceof LookupError,
        () =>
          Effect.sync(() => {
            entry(repo).no_actions_until = now() + HOUR_S
            return { runs: null, changed: false }
          }),
      ),
    )
  })

/** One look at a repo: its pull requests and Actions runs, recording what changed. How many records. */
const poll = (repo: string) =>
  Effect.gen(function* polling() {
    const r = entry(repo)
    let out = 0
    const pulls = yield* pullsSince(repo, r.max_updated)
    const { runs, changed: runsChanged } = yield* runsOf(repo)
    const t = now()
    const fresh = new Map<string, Facts>()
    if (pulls !== null) {
      for (const p of pulls) {
        const e = entityOf(repo, p.number)
        const prior = known.get(e)
        if (!prior && p.state !== "open" && (ms(p.updated_at) ?? 0) < (t - DAY_S) * 1000) {
          // Long settled before it watched: nothing to wake anyone for.
          continue
        }
        fresh.set(e, factsOf(repo, p, prior))
      }
      let top = ""
      for (const p of pulls) {
        if ((p.updated_at ?? "") > top) {
          top = p.updated_at ?? ""
        }
      }
      if (top !== "" && (!filled(r.max_updated) || top > r.max_updated)) {
        r.max_updated = top
      }
    }
    // Checks: the runs on each open pull request's head commit.
    const all = runs?.workflow_runs ?? []
    const bySha = Map.groupBy(all, (run) => run.head_sha ?? null)
    const running = all.some((run) => run.status !== "completed")
    if (runsChanged || fresh.size > 0) {
      const candidates = new Map(
        [...known].filter(([, f]) => f.repo === repo && f.state === "open"),
      )
      for (const [e, f] of fresh) {
        if (f.state === "open") {
          candidates.set(e, f)
        }
      }
      for (const [e, f] of candidates) {
        const { verdict, rows } = rollup(bySha.get(f.head_sha) ?? [])
        if (verdict !== null && (verdict !== f.checks || !same(rows, f.checks_runs))) {
          fresh.set(e, { ...(fresh.get(e) ?? f), checks: verdict, checks_runs: rows })
        }
      }
    }
    for (const f of (yield* followTrains(repo, fresh)).values()) {
      if (yield* consider(f)) {
        out += 1
      }
    }
    const busy = [...known.values()].some(
      (f) => f.repo === repo && (f.updated_at ?? 0) > (t - BUSY_FOR_S) * 1000,
    )
    if (busy || running) {
      r.busy_until = t + (busy ? BUSY_FOR_S : RUNNING_FOR_S)
    }
    r.polled_at = Math.trunc(t * 1000)
    r.watched_since ??= Math.trunc(t * 1000)
    r.error = null
    r.next = t + (t < r.busy_until ? BUSY_S : QUIET_S)
    stats.polls += 1
    stats.last_poll_at = Math.trunc(t * 1000)
    return out
  })

/** The repos to watch: where the user's account authored a pull request lately, the extra ones, and the ones a pr call named. */
const discover = Effect.gen(function* discovering() {
  const t = now()
  if (t < discovered.next) {
    return
  }
  // A failed look tries again in a minute.
  discovered.next = t + RETRY_S
  const since = Temporal.Instant.fromEpochMilliseconds(Math.trunc((t - WINDOW_DAYS * DAY_S) * 1000))
    .toString()
    .slice(0, "YYYY-MM-DD".length)
  const { parsed: found } = yield* getAs(Found, "search/issues", {
    q: `is:pr author:@me updated:>=${since}`,
    per_page: 100,
    sort: "updated",
  })
  const names = new Set(
    (found.items ?? []).flatMap((i) => {
      const url = i.repository_url ?? ""
      const at = url.indexOf(REPOS)
      return at === -1 ? [] : [url.slice(at + REPOS.length)]
    }),
  )
  if (!filled(stats.login)) {
    const user = yield* Effect.option(getAs(User, "user"))
    if (Option.isSome(user)) {
      stats.login = user.value.parsed.login ?? null
    }
  }
  for (const [repo, until] of asked) {
    if (until < t) {
      asked.delete(repo)
    }
  }
  for (const repo of [...EXTRA, ...asked.keys()]) {
    names.add(repo)
  }
  for (const repo of repos.keys()) {
    if (!names.has(repo)) {
      repos.delete(repo)
    }
  }
  for (const repo of names) {
    entry(repo)
  }
  discovered.at = t
  discovered.next = t + DISCOVER_S
  discovered.repos = [...names].toSorted()
})

const due = () => {
  const t = now()
  return [...repos]
    .filter(([, r]) => r.next <= t)
    .toSorted(([a, x], [b, y]) => x.next - y.next || (a < b ? -1 : 1))
    .map(([name]) => name)
}

/** Whatever went wrong, as the error Python caught. */
const squashed = (cause: Cause.Cause<unknown>) => Cause.squash(cause)

/** One pass of the loop: find the repos (now and then), poll the ones that are due. */
const tick = Effect.gen(function* ticking() {
  let ok = yield* discover.pipe(
    Effect.as(true),
    Effect.catchCause((cause) =>
      Effect.sync(() => {
        failed("finding repos", squashed(cause))
        return false
      }),
    ),
  )
  for (const repo of due()) {
    const polled = yield* poll(repo).pipe(
      Effect.as(true),
      Effect.catchCause((cause) =>
        Effect.sync(() => {
          const e = squashed(cause)
          failed(repo, e)
          const r = repos.get(repo)
          if (r) {
            r.error = (e instanceof Error ? e.message : String(e)).slice(0, ERROR_LENGTH)
            r.next = now() + QUIET_S
          }
          return false
        }),
      ),
    )
    ok &&= polled
  }
  if (ok) {
    // A clean pass: what failed before has come right (last_failure keeps it).
    stats.error = null
  }
  save()
})

let saved: string | null = null

export const save = () => {
  const t = now()
  const old = (t - KEEP_DAYS * DAY_S) * 1000
  for (const [e, f] of known) {
    if (f.state !== "open" && (f.updated_at ?? 0) < old) {
      known.delete(e)
    }
  }
  for (const [e, got] of landed) {
    if (got.noted < old) {
      landed.delete(e)
    }
  }
  const body = JSON.stringify({
    asked: Object.fromEntries(asked),
    known: Object.fromEntries(known),
    landed: Object.fromEntries(landed),
    repos: Object.fromEntries(
      [...repos].map(([k, v]) => [
        k,
        { max_updated: v.max_updated, watched_since: v.watched_since },
      ]),
    ),
  })
  if (body === saved) {
    return
  }
  mkdirSync(HOME, { recursive: true, mode: 0o700 })
  writeFileAtomic(STATE, body)
  saved = body
}

const Saved = Schema.Struct({
  known: Schema.optionalKey(Schema.NullOr(Schema.Record(Schema.String, Schema.Unknown))),
  asked: Schema.optionalKey(Schema.NullOr(Schema.Record(Schema.String, Schema.Finite))),
  landed: Schema.optionalKey(Schema.NullOr(Schema.Record(Schema.String, Schema.Unknown))),
  repos: Schema.optionalKey(
    Schema.NullOr(
      Schema.Record(
        Schema.String,
        Schema.Struct({
          max_updated: Schema.optionalKey(Schema.NullOr(Schema.String)),
          watched_since: Schema.optionalKey(Schema.NullOr(Schema.Finite)),
        }),
      ),
    ),
  ),
})
const readSaved = Schema.decodeUnknownOption(Schema.fromJsonString(Saved))
const factsFrom = Schema.decodeUnknownOption(Facts)
const landedFrom = Schema.decodeUnknownOption(Landed)

const load = () => {
  const got = existsSync(STATE) ? readSaved(readFileSync(STATE, "utf-8")) : Option.none()
  if (Option.isNone(got)) {
    return
  }
  const s = got.value
  for (const [e, raw] of Object.entries(s.known ?? {})) {
    const f = factsFrom(raw)
    if (Option.isSome(f)) {
      known.set(e, f.value)
    }
  }
  for (const [repo, until] of Object.entries(s.asked ?? {})) {
    asked.set(repo, until)
  }
  for (const [e, raw] of Object.entries(s.landed ?? {})) {
    const l = landedFrom(raw)
    if (Option.isSome(l)) {
      landed.set(e, l.value)
    }
  }
  for (const [repo, kept] of Object.entries(s.repos ?? {})) {
    const r = entry(repo)
    r.max_updated = kept.max_updated ?? null
    r.watched_since = kept.watched_since ?? null
  }
}

/** Says how it is, in vitals/github-prs. */
const heartbeat = Effect.gen(function* beating() {
  const t = now()
  const n = repos.size
  const busy = [...repos.values()].filter((r) => t < r.busy_until).length
  const facts = { ...stats, repos: n, busy_repos: busy, known: known.size }
  const said = `Watching ${n} repos (${busy} busy), ${known.size} pull requests known`
  const emit = yield* Emit
  yield* emit(
    Heartbeat.record({
      entity: `sense:${NAME}`,
      summary: filled(stats.error) ? `Failing: ${stats.error}` : said,
      facts,
    }),
  )
})

/** A sense that dies takes the server with it, so its host starts it again. */
const guarded = (name: string, loop: Effect.Effect<never, unknown, Emit>) =>
  loop.pipe(
    Effect.catchCause((cause) =>
      Effect.sync(() => {
        log(`${name} stopped:\n${Cause.pretty(cause)}`)
        process.exit(1)
      }),
    ),
  )

export const watch = guarded(
  "watch",
  Effect.andThen(Effect.sync(load), Effect.forever(Effect.andThen(tick, Effect.sleep("1 second")))),
)

export const beat = guarded(
  "beat",
  Effect.andThen(
    Effect.sleep(FIRST_BEAT_S * 1000),
    Effect.forever(Effect.andThen(heartbeat, Effect.sleep(HEARTBEAT_S * 1000))),
  ),
)
