// GitHub's REST API as the user's gh login, ETag-cached: an answer that hasn't changed (a 304) costs
// nothing against the rate limit, and says so (changed false).
import { Effect, Schema } from "effect"

/** An environment variable, when it's set to something: as Python's `os.environ.get(name) or …`. */
export const env = (name: string) => {
  const v = process.env[name]
  return v === undefined || v === "" ? undefined : v
}

export const API = env("GITHUB_PRS_API") ?? "https://api.github.com"

/** GitHub has no such thing, or this login can't see it. */
export class LookupError extends Error {
  override readonly name = "LookupError"
}

export class RuntimeError extends Error {
  override readonly name = "RuntimeError"
}

export interface Stats {
  started_at: number
  polls: number
  requests: number
  not_modified: number
  observed: number
  rate_remaining: number | null
  rate_reset: number | null
  last_poll_at: number | null
  last_change_at: number | null
  error: string | null
  last_failure: string | null
  last_failure_at: number | null
  login: string | null
}

/** How it is: kept in its status and heartbeat. */
export const stats: Stats = {
  started_at: Date.now(),
  polls: 0,
  requests: 0,
  not_modified: 0,
  observed: 0,
  rate_remaining: null,
  rate_reset: null,
  last_poll_at: null,
  last_change_at: null,
  error: null,
  last_failure: null,
  last_failure_at: null,
  login: null,
}

const FETCH_MS = 20_000
const CACHED = 300
const DROPPED = 100

/** The world outside: one HTTP GET to GitHub, as its status, headers and body. */
const fetchOnce = (url: string, headers: Record<string, string>) =>
  Effect.tryPromise({
    try: async () => {
      const res = await fetch(url, { headers, signal: AbortSignal.timeout(FETCH_MS) })
      return { status: res.status, headers: res.headers, raw: await res.text() }
    },
    catch: (e) => (e instanceof Error ? e : new RuntimeError(String(e))),
  })

const tokenFromGh = Effect.promise(async () => {
  try {
    const p = Bun.spawn(["gh", "auth", "token"], {
      stdout: "pipe",
      stderr: "ignore",
      timeout: FETCH_MS,
    })
    const out = await new Response(p.stdout).text()
    return (await p.exited) === 0 ? out.trim() : ""
  } catch {
    return ""
  }
})

let token = ""
const cache = new Map<string, readonly [string, unknown]>()

const tokenOf = (again: boolean) =>
  Effect.gen(function* tokening() {
    if (again || token === "") {
      const given = env("GH_TOKEN") ?? env("GITHUB_TOKEN") ?? ""
      const gh = again || given === "" ? yield* tokenFromGh : ""
      token = again ? gh || given : given || gh
      if (token === "") {
        return yield* Effect.fail(
          new RuntimeError(
            "no GitHub token: gh auth token gave nothing (gh auth login on this Mac)",
          ),
        )
      }
    }
    return token
  })

const parseJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))

/** What GitHub answers, and whether it's new since the last time this was asked. */
export const get = (
  pathname: string,
  params: Readonly<Record<string, string | number>> = {},
): Effect.Effect<{ readonly parsed: unknown; readonly changed: boolean }, Error> =>
  Effect.gen(function* getting() {
    let url = `${API}/${pathname.replace(/^\/+/u, "")}`
    const query = new URLSearchParams()
    for (const [k, v] of Object.entries(params)) {
      query.append(k, String(v))
    }
    if (query.size > 0) {
      url += `?${query.toString()}`
    }
    for (const attempt of [1, 2]) {
      const headers: Record<string, string> = {
        Authorization: `Bearer ${yield* tokenOf(attempt === 2)}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "switchboard-github-prs",
      }
      const cached = cache.get(url)
      if (cached) {
        const [etag] = cached
        headers["If-None-Match"] = etag
      }
      const { status, headers: got, raw } = yield* fetchOnce(url, headers)
      stats.requests += 1
      const remaining = got.get("x-ratelimit-remaining")
      if (remaining !== null && remaining !== "") {
        stats.rate_remaining = Math.trunc(Number(remaining))
        stats.rate_reset = Math.trunc(Number(got.get("x-ratelimit-reset") ?? "0")) * 1000 || null
      }
      if (status === 401 && attempt === 1) {
        continue
      }
      if (status === 304 && cached) {
        stats.not_modified += 1
        return { parsed: cached[1], changed: false }
      }
      if (status === 404) {
        return yield* Effect.fail(
          new LookupError(`GitHub has no ${pathname} (or this login can't see it)`),
        )
      }
      if (status >= 400) {
        return yield* Effect.fail(
          new RuntimeError(`GitHub GET ${pathname} answered ${status}: ${raw.slice(0, 200)}`),
        )
      }
      const parsed = raw === "" ? {} : yield* parseJson(raw)
      const etag = got.get("etag")
      if (etag !== null && etag !== "") {
        cache.set(url, [etag, parsed])
        if (cache.size > CACHED) {
          for (const k of [...cache.keys()].slice(0, DROPPED)) {
            cache.delete(k)
          }
        }
      }
      return { parsed, changed: true }
    }
    return yield* Effect.fail(new RuntimeError("GitHub: not signed in (gh auth login on this Mac)"))
  })

/** `get`, its answer decoded as `schema`. */
export const getAs = <A>(
  schema: Schema.Codec<A, unknown>,
  pathname: string,
  params: Readonly<Record<string, string | number>> = {},
): Effect.Effect<{ readonly parsed: A; readonly changed: boolean }, Error> =>
  Effect.flatMap(get(pathname, params), ({ parsed, changed }) =>
    Effect.map(Schema.decodeUnknownEffect(schema)(parsed), (p) => ({ parsed: p, changed })),
  )

// What it reads of GitHub's answers; whatever else they hold, it ignores.
const text = Schema.optionalKey(Schema.NullOr(Schema.String))
const int = Schema.optionalKey(Schema.NullOr(Schema.Int))
const side = Schema.Struct({
  ref: text,
  sha: text,
  repo: Schema.optionalKey(Schema.NullOr(Schema.Struct({ full_name: text }))),
})

export const Pull = Schema.Struct({
  number: Schema.Int,
  title: text,
  state: Schema.String,
  draft: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
  user: Schema.optionalKey(Schema.NullOr(Schema.Struct({ login: text }))),
  head: Schema.optionalKey(Schema.NullOr(side)),
  base: Schema.optionalKey(Schema.NullOr(side)),
  html_url: text,
  created_at: text,
  updated_at: text,
  merged_at: text,
  closed_at: text,
  merge_commit_sha: text,
  body: text,
})
export type Pull = typeof Pull.Type

export const Pulls = Schema.Array(Pull)

export const Run = Schema.Struct({
  id: int,
  workflow_id: int,
  name: text,
  head_sha: text,
  status: text,
  conclusion: text,
  run_attempt: int,
  created_at: text,
  html_url: text,
})
export type Run = typeof Run.Type

export const Runs = Schema.Struct({ workflow_runs: Schema.optionalKey(Schema.Array(Run)) })
export type Runs = typeof Runs.Type

export const Found = Schema.Struct({
  items: Schema.optionalKey(Schema.Array(Schema.Struct({ repository_url: text }))),
})
export const User = Schema.Struct({ login: text })
export const Comments = Schema.Array(Schema.Struct({ body: text }))
export const Compared = Schema.Struct({ status: text })
