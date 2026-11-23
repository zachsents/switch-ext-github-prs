// github-prs: pull requests in the user's active GitHub repos, as entities, so work can wake when one
// merges. Each pull request is the entity `pr:<owner>/<repo>#<number>` (lowercase), and it records an
// observation in github/prs only when something that matters changes: opened, pushed, retitled, draft
// or ready, merged, closed, reopened, or its checks (the latest Actions run of each workflow on its
// head commit) start, pass, fail or are cancelled. Merge trains (scripts/train.sh) are followed: which
// open train carries each pull request now, what replaced a closed train, and every pull request a
// train landed shows merged, whatever GitHub shows for it.
import { Effect, Schema } from "effect"
import { defineApp, tool } from "switch-ext-kit"

import { env, getAs, Pull, stats } from "./github.ts"
import {
  asked,
  consider,
  entityOf,
  entry,
  factsOf,
  filled,
  followTrains,
  given,
  Heartbeat,
  known,
  NAME,
  now,
  Pr,
  repos,
  rollup,
} from "./prs.ts"
import type { Emit, Facts } from "./prs.ts"
import { BUSY_FOR_S, BUSY_S, discovered, QUIET_S, runsOf, save } from "./sense.ts"

const DEFAULT_REPO = env("GITHUB_PRS_DEFAULT_REPO") ?? ""
/** A repo a `pr` call named is watched this long after, whoever's pull requests it has. */
const ASKED_FOR_S = 3 * 86_400
const LIMIT = 30
const MOST = 200

const INSTRUCTIONS = `Pull requests as entities work wakes on, sensed within ~10 s: pr:<owner>/<repo>#<number> (lowercase, like pr:owner/repo#371), facts.state open|merged|closed, facts.checks running|passed|failed|cancelled. To act the moment one merges or closes, the brain tool's expect with when \`entities | where id == "pr:owner/repo#371" and facts.state != "open"\`; for its CI, \`... and facts.head_sha == "<sha>" and facts.checks in ("passed", "failed", "cancelled")\` (failed as soon as one workflow fails). pr(number, repo) reads one fresh and answers with those queries filled in; prs lists what's sensed; status says how the sense is.

Merge trains (scripts/train.sh: a train/ branch merging several pull requests onto main, landed by main fast-forwarding to it) are followed through every train that replaces another. An open pull request a train carries at its newest commit has facts.train, that train's number. Once a train lands, each one it carried at its newest commit is merged, with facts.landed_by naming the train, whatever GitHub shows for it (a rider closed by hand, or not marked yet); so is any pull request closed unmerged whose newest commit main already holds. So to act once a pull request is on main, wait on its own merged query, whichever train ends up landing it, never on a train, which may be replaced. A train itself has facts.carries (the numbers it carries, in order) and carried_heads (each one's commit, short), and once it closes unmerged, facts.replaced_by (the train that took its place).

Each observation (in github/prs) carries repo, number, ref (switchboard#371), title, state, draft, author, head_ref, head_sha, base_ref, url, created_at, updated_at, merged_at, closed_at, merge_commit, checks with checks_runs (the latest Actions run of each workflow on its head commit), train, landed_by, carries, carried_heads, replaced_by, and changed (seen, opened, pushed, retitled, draft, ready, merged, closed, reopened, checks …, on train #N, off its train, replaced by #N). It's recorded only when one of those changes, never twice for the same state.

It watches every repo where the user's account authored a pull request in the last two weeks (looked for every 10 min), and any repo a pr call names for three days after, polling each with ETags (free when nothing changed): every 10 s while it's busy, every 2 min while it's quiet.`

const waitsFor = (e: string, f: Facts) => ({
  done: `entities | where id == "${e}" and facts.state != "open"`,
  merged: `entities | where id == "${e}" and facts.state == "merged"`,
  ...(f.state === "open" &&
    filled(f.head_sha) && {
      checks: `entities | where id == "${e}" and facts.head_sha == "${f.head_sha}" and facts.checks in ("passed", "failed", "cancelled")`,
    }),
})

const pr = tool(
  "pr",
  {
    description:
      "One pull request as GitHub has it now (asked fresh, and recorded if it changed), with its entity and the `when` queries that wake work once it's merged or closed (done), merged, or its checks on this head finish (checks). repo is owner/name (default owner/repo); naming one watches it for three days whoever's pull requests it has.",
    readOnly: true,
    input: Schema.Struct({ number: Schema.Int, repo: Schema.optionalKey(Schema.String) }),
  },
  (input) =>
    Effect.gen(function* reading() {
      const repo = (input.repo ?? DEFAULT_REPO).trim().replaceAll(/^\/+|\/+$/gu, "")
      if (repo.split("/").length !== 2) {
        return yield* Effect.fail(new Error(`repo is owner/name, like owner/repo, not '${repo}'`))
      }
      const e = entityOf(repo, input.number)
      const { parsed: p } = yield* getAs(Pull, `repos/${repo}/pulls/${input.number}`)
      const full = p.base?.repo?.full_name ?? repo
      const prior = known.get(e)
      asked.set(full, now() + ASKED_FOR_S)
      const r = entry(full)
      r.busy_until = Math.max(r.busy_until, now() + BUSY_FOR_S)
      r.next = Math.min(r.next, now() + BUSY_S)
      let f = factsOf(full, p, prior)
      if (f.state === "open" && filled(f.head_sha)) {
        const { runs } = yield* runsOf(full, { head_sha: f.head_sha })
        const { verdict, rows } = rollup(runs?.workflow_runs ?? [])
        if (verdict !== null) {
          f = { ...f, checks: verdict, checks_runs: rows }
        }
      }
      const more = yield* followTrains(full, new Map([[e, f]]))
      f = more.get(e) ?? f
      more.delete(e)
      const recorded = yield* consider(f)
      for (const g of more.values()) {
        yield* consider(g)
      }
      save()
      return { entity: e, ...f, recorded: recorded !== null, when: waitsFor(e, f) }
    }),
)

const prs = tool(
  "prs",
  {
    description:
      "The pull requests it has sensed, the most recently changed first: repo (owner/name) and state (open, merged, closed) narrow them.",
    readOnly: true,
    input: Schema.Struct({
      repo: Schema.optionalKey(Schema.String),
      state: Schema.optionalKey(Schema.String),
      limit: Schema.optionalKey(Schema.Int),
    }),
  },
  (input) =>
    Effect.sync(() => {
      const rows = [...known]
        .filter(
          ([, f]) =>
            (!filled(input.repo) || f.repo.toLowerCase() === input.repo.toLowerCase()) &&
            (!filled(input.state) || f.state === input.state),
        )
        .map(([e, f]) => ({
          entity: e,
          ref: f.ref,
          title: f.title,
          state: f.state,
          draft: f.draft,
          checks: f.checks,
          head_sha: f.head_sha,
          train: f.train,
          landed_by: f.landed_by,
          replaced_by: f.replaced_by,
          updated_at: f.updated_at,
          merged_at: f.merged_at,
          url: f.url,
        }))
        .toSorted((a, b) => (b.updated_at ?? 0) - (a.updated_at ?? 0))
      return {
        count: rows.length,
        prs: rows.slice(0, Math.max(1, Math.min(given(input.limit) ? input.limit : LIMIT, MOST))),
      }
    }),
)

const status = tool(
  "status",
  {
    description:
      "How the sense is: the repos it watches (each polled when, how often, its last error), its requests to GitHub and how many came back unchanged and free, the rate limit left, and its last error.",
    readOnly: true,
  },
  () =>
    Effect.sync(() => {
      const t = now()
      const watched = [...repos]
        .toSorted(([a], [b]) => (a < b ? -1 : 1))
        .map(([name, r]) => ({
          repo: name,
          polled_at: r.polled_at,
          every_s: t < r.busy_until ? BUSY_S : QUIET_S,
          open: [...known.values()].filter((f) => f.repo === name && f.state === "open").length,
          error: r.error,
          asked: asked.has(name),
        }))
      return {
        ...stats,
        repos: watched,
        known: known.size,
        discovered_at: Math.trunc(discovered.at * 1000) || null,
      }
    }),
)

export const app = defineApp<Emit>({
  name: NAME,
  version: "1",
  instructions: INSTRUCTIONS,
  tools: [pr, prs, status],
  events: [Pr, Heartbeat],
})
