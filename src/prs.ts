// What a pull request is, what changed, and merge trains: what each carries, what replaced it, and
// what landed with it.
import { createHash } from "node:crypto"
import { Context, Effect, Schema } from "effect"
import { defineRecord } from "switch-ext-kit"
import type { Draft } from "switch-ext-kit"

import { Comments, Compared, getAs, stats } from "./github.ts"
import type { Pull, Run } from "./github.ts"
import { pyJson } from "./pyjson.ts"

export const NAME = "github-prs"

export const Pr = defineRecord(`${NAME}.pr`, { kind: "observation", stream: "github/prs" })
export const Heartbeat = defineRecord(`${NAME}.heartbeat`, {
  kind: "observation",
  stream: `vitals/${NAME}`,
})

/** Where its records go: the extension's outbox. */
export class Emit extends Context.Service<Emit, (draft: Draft) => Effect.Effect<void>>()(
  "github-prs/Emit",
) {}

export const log = (words: string) => {
  process.stderr.write(`${words}\n`)
}

const FAILING = new Set(["failure", "timed_out", "startup_failure", "action_required"])
const FINE = new Set(["success", "skipped", "neutral"])
const DAY_S = 86_400

/** Epoch seconds, as its schedule keeps them. */
export const now = () => Date.now() / 1000

/** A string with something in it, as Python's truthiness has it. */
export const filled = (s: string | null | undefined): s is string =>
  s !== null && s !== undefined && s !== ""

/** A number other than 0, as Python's truthiness has it. */
export const given = (n: number | null | undefined): n is number =>
  n !== null && n !== undefined && n !== 0

export const ms = (iso: string | null | undefined) =>
  filled(iso) ? Temporal.Instant.from(iso).epochMilliseconds : null

/** Python's `str()` of a value in a sentence. */
const said = (v: string | number | null) => (v === null ? "None" : String(v))

/** The first `n` characters, as Python slices them. */
const clip = (s: string, n: number) => {
  let out = ""
  let count = 0
  for (const ch of s) {
    if (count === n) {
      break
    }
    out += ch
    count += 1
  }
  return out
}

export const CheckRun = Schema.Struct({
  workflow: Schema.NullOr(Schema.String),
  status: Schema.NullOr(Schema.String),
  conclusion: Schema.NullOr(Schema.String),
  run: Schema.NullOr(Schema.Finite),
  attempt: Schema.NullOr(Schema.Finite),
  url: Schema.NullOr(Schema.String),
})
export type CheckRun = typeof CheckRun.Type

/** A pull request as its entity keeps it. */
export const Facts = Schema.Struct({
  repo: Schema.String,
  number: Schema.Finite,
  ref: Schema.String,
  title: Schema.NullOr(Schema.String),
  state: Schema.String,
  draft: Schema.Boolean,
  author: Schema.NullOr(Schema.String),
  head_ref: Schema.NullOr(Schema.String),
  head_sha: Schema.NullOr(Schema.String),
  base_ref: Schema.NullOr(Schema.String),
  url: Schema.NullOr(Schema.String),
  created_at: Schema.NullOr(Schema.Finite),
  updated_at: Schema.NullOr(Schema.Finite),
  merged_at: Schema.NullOr(Schema.Finite),
  closed_at: Schema.NullOr(Schema.Finite),
  merge_commit: Schema.NullOr(Schema.String),
  checks: Schema.NullOr(Schema.String),
  checks_runs: Schema.NullOr(Schema.Array(CheckRun)),
  carries: Schema.NullOr(Schema.Array(Schema.Finite)),
  carried_heads: Schema.NullOr(Schema.Record(Schema.String, Schema.String)),
  replaced_by: Schema.NullOr(Schema.Finite),
  train: Schema.NullOr(Schema.Finite),
  landed_by: Schema.NullOr(Schema.Finite),
})
export type Facts = typeof Facts.Type

/** On its base though GitHub may not say merged. */
export const Landed = Schema.Struct({
  head: Schema.String,
  by: Schema.NullOr(Schema.Finite),
  at: Schema.NullOr(Schema.Finite),
  commit: Schema.NullOr(Schema.String),
  noted: Schema.Finite,
})
export type Landed = typeof Landed.Type

export interface Watched {
  /** Epoch seconds. */
  next: number
  busy_until: number
  polled_at: number | null
  error: string | null
  max_updated: string | null
  watched_since: number | null
  no_actions_until?: number
}

/** full_name -> how it's watched. */
export const repos = new Map<string, Watched>()
/** entity -> facts as last recorded. */
export const known = new Map<string, Facts>()
/** full_name -> until (epoch s): a repo a `pr` call named. */
export const asked = new Map<string, number>()
/** entity -> on its base though GitHub may not say merged. */
export const landed = new Map<string, Landed>()
/** Trains closed unmerged whose replacement it has looked for since it started. */
const looked = new Set<string>()

/** What it keeps of a repo it watches (made the first time). */
export const entry = (repo: string) => {
  const got = repos.get(repo)
  if (got) {
    return got
  }
  const made: Watched = {
    next: 0,
    busy_until: 0,
    polled_at: null,
    error: null,
    max_updated: null,
    watched_since: null,
  }
  repos.set(repo, made)
  return made
}

const errorOf = (e: unknown) => (e instanceof Error ? `${e.name}: ${e.message}` : String(e))

export const failed = (where: string, e: unknown) => {
  const error = clip(`${where}: ${errorOf(e)}`, 400)
  stats.error = error
  stats.last_failure = error
  stats.last_failure_at = Date.now()
  log(error)
}

export const entityOf = (repo: string, number: number) =>
  `pr:${repo.toLowerCase()}#${Math.trunc(number)}`

const TRAIN_LINE = /^- #(?<number>\d+) \((?<head>[0-9a-f]{7,40})\)/gmu
const REPLACED = /\bReplaced by #(?<number>\d+)/u

const isTrain = (f: Facts) => (f.head_ref ?? "").startsWith("train/")

/**
 * What a train says it carries, in order, as (number, its commit as carried, short): scripts/train.sh
 * writes a line `- #N (abcd1234): title` for each, and none for what it set aside.
 */
const carried = (body: string | null | undefined) =>
  Array.from(
    (body ?? "").matchAll(TRAIN_LINE),
    (m) => [Number(m.groups?.number), m.groups?.head ?? ""] as const,
  )

/**
 * f with what GitHub may not show: merged once main holds its newest commit (a train it rode landed,
 * or its commit reached main another way), with the train that landed it.
 */
const overlay = (e: string, f: Facts): Facts => {
  const got = landed.get(e)
  const out = { ...f, landed_by: null }
  if (got === undefined || !filled(out.head_sha) || !out.head_sha.startsWith(got.head)) {
    return out
  }
  if (out.state === "merged") {
    return { ...out, landed_by: got.by }
  }
  return {
    ...out,
    landed_by: got.by,
    state: "merged",
    merged_at: out.merged_at ?? got.at,
    merge_commit: got.commit,
  }
}

/**
 * A pull request as GitHub lists it, as the facts its entity keeps (its checks carried over while
 * its head commit is the same).
 */
export const factsOf = (repo: string, p: Pull, prior: Facts | undefined): Facts => {
  const state = filled(p.merged_at) ? "merged" : p.state
  const head = p.head ?? {}
  const headSha = head.sha ?? null
  const same = prior !== undefined && prior.head_sha === headSha
  const headRef = head.ref ?? null
  const rows = (headRef ?? "").startsWith("train/") ? carried(p.body) : null
  const f: Facts = {
    repo,
    number: p.number,
    ref: `${repo.split("/").at(-1) ?? ""}#${p.number}`,
    title: p.title ?? null,
    state,
    draft: Boolean(p.draft),
    author: p.user?.login ?? null,
    head_ref: headRef,
    head_sha: headSha,
    base_ref: p.base?.ref ?? null,
    url: p.html_url ?? null,
    created_at: ms(p.created_at),
    updated_at: ms(p.updated_at),
    merged_at: ms(p.merged_at),
    closed_at: ms(p.closed_at),
    merge_commit: state === "merged" ? (p.merge_commit_sha ?? null) : null,
    checks: same ? prior.checks : null,
    checks_runs: same ? prior.checks_runs : null,
    carries: rows && rows.map(([n]) => n),
    carried_heads: rows && Object.fromEntries(rows.map(([n, h]) => [String(n), h])),
    replaced_by: state === "closed" ? (prior?.replaced_by ?? null) : null,
    // Which train carries it now is the repo's to say (followTrains); until then, what it was.
    train: state === "open" && same ? prior.train : null,
    landed_by: null,
  }
  return overlay(entityOf(repo, p.number), f)
}

/** Notes that main holds number's commit head (landed by train by, or null), so it shows merged. */
const land = (
  repo: string,
  number: number,
  head: string,
  by: number | null,
  at: number | null,
  commit: string | null,
) => {
  const e = entityOf(repo, number)
  landed.set(e, { head, by, at, commit, noted: Date.now() })
  return e
}

/** The train that took a closed train's place, from scripts/train.sh's "Replaced by #N." comment. */
const replacement = (repo: string, number: number) =>
  Effect.gen(function* replacing() {
    const got = yield* Effect.result(
      getAs(Comments, `repos/${repo}/issues/${Math.trunc(number)}/comments`, { per_page: 100 }),
    )
    if (got._tag === "Failure") {
      failed(`reading #${number}'s comments`, got.failure)
      return null
    }
    for (const c of got.success.parsed.toReversed()) {
      const m = REPLACED.exec(c.body ?? "")
      if (m) {
        return Number(m.groups?.number)
      }
    }
    return null
  })

/** Whether base (a branch) holds commit head already: GitHub's compare says head is behind it or the same. */
const holds = (repo: string, base: string, head: string) =>
  Effect.gen(function* comparing() {
    const got = yield* Effect.result(
      getAs(Compared, `repos/${repo}/compare/${encodeURIComponent(base)}...${head}`, {
        per_page: 1,
      }),
    )
    if (got._tag === "Failure") {
      failed(`comparing ${head.slice(0, 8)} with ${base}`, got.failure)
      return false
    }
    const { status } = got.success.parsed
    return status === "behind" || status === "identical"
  })

/** Puts in fresh, as they now show, the pull requests just noted as landed. */
const relanded = (fresh: Map<string, Facts>, entities: ReadonlySet<string>) => {
  for (const e of entities) {
    const f = fresh.get(e) ?? known.get(e)
    if (f) {
      fresh.set(e, overlay(e, f))
    }
  }
}

/**
 * What merge trains say of the pull requests in fresh (entity -> facts as GitHub has them now) and
 * of the rest of repo's, as more of fresh: a train that landed lands what it carried at their newest
 * commits; one closed unmerged names what replaced it; one closed unmerged that isn't a train is
 * merged if main holds its commit; and each open one carries the number of the newest open train
 * carrying it at its newest commit (so it follows a train that replaces another).
 */
export const followTrains = (repo: string, fresh: Map<string, Facts>) =>
  Effect.gen(function* following() {
    const before = new Map([...fresh.keys()].map((e) => [e, known.get(e)]))
    let touched = new Set<string>()
    for (const [e, f] of fresh) {
      const was = before.get(e)?.state
      if (isTrain(f) && f.state === "merged" && was !== "merged") {
        for (const n of f.carries ?? []) {
          touched.add(
            land(
              repo,
              n,
              f.carried_heads?.[String(n)] ?? "",
              f.number,
              f.merged_at,
              f.merge_commit ?? f.head_sha,
            ),
          )
        }
      } else if (
        isTrain(f) &&
        f.state === "closed" &&
        !given(f.replaced_by) &&
        !looked.has(e) &&
        (was !== "closed" || (f.closed_at ?? 0) > (now() - DAY_S) * 1000)
      ) {
        // Once a run, so one closed before it started (or before it read comments) is named too.
        looked.add(e)
        fresh.set(e, { ...f, replaced_by: yield* replacement(repo, f.number) })
      }
    }
    relanded(fresh, touched)
    const mergedTrains = [...known.values(), ...fresh.values()].filter(
      (g) => g.repo === repo && isTrain(g) && g.state === "merged",
    )
    touched = new Set<string>()
    for (const [e, f] of fresh) {
      const was = before.get(e)?.state
      if (
        f.state === "closed" &&
        was !== "closed" &&
        !isTrain(f) &&
        filled(f.head_sha) &&
        filled(f.base_ref) &&
        (yield* holds(repo, f.base_ref, f.head_sha))
      ) {
        const sha = f.head_sha
        const by =
          mergedTrains.find((g) => sha.startsWith(g.carried_heads?.[String(f.number)] ?? "-"))
            ?.number ?? null
        touched.add(land(repo, f.number, sha, by, f.closed_at, sha))
      }
    }
    relanded(fresh, touched)
    // Which open train carries each open pull request at its newest commit: the newest, when two do
    // for a moment as one replaces the other.
    const every = new Map([...known].filter(([, f]) => f.repo === repo))
    for (const [e, f] of fresh) {
      every.set(e, f)
    }
    const on = new Map<number, readonly [number, string]>()
    for (const f of every.values()) {
      if (isTrain(f) && f.state === "open") {
        for (const [n, h] of Object.entries(f.carried_heads ?? {})) {
          const was = on.get(Number(n))
          if (!was || was[0] < f.number) {
            on.set(Number(n), [f.number, h])
          }
        }
      }
    }
    for (const [e, f] of every) {
      if (f.state !== "open" || isTrain(f)) {
        continue
      }
      const carrier = on.get(f.number)
      const want =
        carrier !== undefined && given(carrier[0]) && (f.head_sha ?? "").startsWith(carrier[1])
          ? carrier[0]
          : null
      if (want !== f.train) {
        fresh.set(e, { ...f, train: want })
      }
    }
    return fresh
  })

const MATTERS = [
  "state",
  "draft",
  "head_sha",
  "title",
  "base_ref",
  "checks",
  "train",
  "landed_by",
  "replaced_by",
] as const

const ENDED: Readonly<Record<string, string>> = { merged: "merged", closed: "closed" }
const STANDS: Readonly<Record<string, string>> = { merged: "is merged", closed: "is closed" }

/**
 * The words for what changed: [] when nothing that matters did. A pull request it hasn't seen is
 * "opened" (or "merged", "closed") when it's newer than its repo's watch, else "seen" as it stands.
 */
const whatChanged = (prior: Facts | undefined, f: Facts, watchedSince: number | null) => {
  if (!prior) {
    if (watchedSince === null || (f.created_at ?? 0) < watchedSince) {
      return ["seen"]
    }
    return [ENDED[f.state] ?? "opened"]
  }
  const out: string[] = []
  if (prior.state !== f.state) {
    out.push(ENDED[f.state] ?? "reopened")
  }
  if (prior.head_sha !== f.head_sha && f.state === "open") {
    out.push("pushed")
  }
  if (prior.title !== f.title) {
    out.push("retitled")
  }
  if (prior.draft !== f.draft) {
    out.push(f.draft ? "draft" : "ready")
  }
  if (prior.base_ref !== f.base_ref) {
    out.push(`rebased onto ${said(f.base_ref)}`)
  }
  if (prior.checks !== f.checks && filled(f.checks)) {
    out.push(`checks ${f.checks}`)
  }
  if (prior.train !== f.train && f.state === "open") {
    out.push(given(f.train) ? `on train #${f.train}` : "off its train")
  }
  if (prior.replaced_by !== f.replaced_by && given(f.replaced_by)) {
    out.push(`replaced by #${f.replaced_by}`)
  }
  if (prior.landed_by !== f.landed_by && given(f.landed_by) && !out.includes("merged")) {
    out.push(`landed with train #${f.landed_by}`)
  }
  return out
}

const summaryOf = (f: Facts, changed: readonly string[]) => {
  const words: Record<string, string> = {
    seen: STANDS[f.state] ?? "is open",
    opened: "opened",
    merged: "merged",
    closed: "closed without merging",
    reopened: "reopened",
    pushed: `has a new commit ${(f.head_sha ?? "").slice(0, 7)}`,
    retitled: "retitled",
    draft: "back to draft",
    ready: "ready for review",
    "checks running": "checks running",
    "checks passed": "checks passed",
    "checks failed": "checks failed",
    "checks cancelled": "checks cancelled",
  }
  if (given(f.landed_by)) {
    words.merged = `merged with train #${f.landed_by}`
  }
  if (given(f.replaced_by)) {
    words.closed = "closed"
  }
  let out = changed.map((c) => words[c] ?? c).join(", ")
  if (
    f.state === "open" &&
    filled(f.checks) &&
    !changed.some((c) => c.startsWith("checks")) &&
    !changed.includes("pushed")
  ) {
    out += ` (checks ${f.checks})`
  }
  return `${f.ref} ${out}: ${said(f.title)}`
}

const BOT = /\[bot\]$|^dependabot|^renovate|^github-actions/iu
const IMPORTANCE_ENDED = 0.5
const IMPORTANCE_OPEN = 0.2
const NAME_LENGTH = 200
const ID_LENGTH = 40

/**
 * Records the pull request's state as an observation of its entity. Its id is the state's own, so
 * the same state sent twice (a restart) is recorded once.
 */
const observe = (f: Facts, changed: readonly string[]) =>
  Effect.gen(function* observing() {
    const e = entityOf(f.repo, f.number)
    const fingerprint = pyJson([e, ...MATTERS.map((k) => f[k]), f.updated_at, f.checks_runs])
    const rid = `pr-${createHash("sha256").update(fingerprint).digest("hex").slice(0, ID_LENGTH)}`
    const data = {
      entity: e,
      kind: "pull-request",
      name: clip(`${f.ref} ${said(f.title)}`, NAME_LENGTH),
      summary: summaryOf(f, changed),
      facts: { ...f, changed },
    }
    const author = f.author ?? ""
    const sender = author
      ? { handle: author.toLowerCase(), via: "github", ...(BOT.test(author) && { agent: true }) }
      : null
    const emit = yield* Emit
    yield* emit(
      Pr.record(data, {
        about: [e],
        id: rid,
        eventId: rid,
        importance: f.state === "open" ? IMPORTANCE_OPEN : IMPORTANCE_ENDED,
        ...(sender && { sender }),
      }),
    )
    known.set(e, f)
    stats.observed += 1
    stats.last_change_at = Date.now()
    return data
  })

/** Records f when what matters about it changed since it was last recorded; the record, else null. */
export const consider = (f: Facts) =>
  Effect.gen(function* considering() {
    const e = entityOf(f.repo, f.number)
    const changed = whatChanged(known.get(e), f, repos.get(f.repo)?.watched_since ?? null)
    if (changed.length === 0) {
      // Keep its times fresh without a record.
      known.set(e, f)
      return null
    }
    return yield* observe(f, changed)
  })

/** Which of two runs of a workflow is the latest. */
const order = (r: Run) => [r.created_at ?? "", r.id ?? 0] as const

/**
 * The checks on one commit, from its Actions runs: the latest run of each workflow, as failed (any
 * failed, even while others run), running, passed (all fine) or cancelled, with the runs.
 */
export const rollup = (runs: readonly Run[]) => {
  const latest = new Map<number | string | null, Run>()
  for (const r of runs) {
    const key = given(r.workflow_id) ? r.workflow_id : (r.name ?? null)
    const was = latest.get(key)
    if (!was) {
      latest.set(key, r)
      continue
    }
    const [at, id] = order(r)
    const [wasAt, wasId] = order(was)
    if (at > wasAt || (at === wasAt && id > wasId)) {
      latest.set(key, r)
    }
  }
  if (latest.size === 0) {
    return { verdict: null, rows: null }
  }
  const all = [...latest.values()]
  const rows: CheckRun[] = all
    .map((r) => ({
      workflow: r.name ?? null,
      status: r.status ?? null,
      conclusion: r.conclusion ?? null,
      run: r.id ?? null,
      attempt: r.run_attempt ?? null,
      url: r.html_url ?? null,
    }))
    .toSorted((a, b) => {
      const x = said(a.workflow)
      const y = said(b.workflow)
      if (x < y) {
        return -1
      }
      return x > y ? 1 : 0
    })
  let verdict = "cancelled"
  if (all.some((r) => r.status === "completed" && FAILING.has(r.conclusion ?? ""))) {
    // As GitHub's own rollup: one failure fails it, whatever else still runs.
    verdict = "failed"
  } else if (all.some((r) => r.status !== "completed")) {
    verdict = "running"
  } else if (all.every((r) => FINE.has(r.conclusion ?? ""))) {
    verdict = "passed"
  }
  return { verdict, rows }
}

/** Whether two values are the same, as Python's `==` on what JSON holds. */
export const same = (a: unknown, b: unknown) => pyJson(a) === pyJson(b)
