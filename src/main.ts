// github-prs as a stdio MCP server: its tools, and the sense that polls GitHub and says how it is.
import { mkdirSync } from "node:fs"
import { Effect, Layer } from "effect"
import { AppEvents, serveStdio } from "switch-ext-kit"
import type { Draft } from "switch-ext-kit"

import { app } from "./app.ts"
import { Emit } from "./prs.ts"
import { beat, HOME, watch } from "./sense.ts"

const sensing = Layer.effect(
  Emit,
  Effect.gen(function* starting() {
    const events = yield* AppEvents
    const emit = (draft: Draft) => Effect.orDie(events.emit(draft))
    yield* Effect.forkScoped(Effect.provideService(watch, Emit, emit))
    yield* Effect.forkScoped(Effect.provideService(beat, Emit, emit))
    return emit
  }),
)

mkdirSync(HOME, { recursive: true, mode: 0o700 })
await serveStdio(app, sensing)
