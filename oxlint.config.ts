import { defineConfig } from "oxlint"
import core from "ultracite/oxlint/core"
import react from "ultracite/oxlint/react"

export default defineConfig({
  extends: [core, react],
  options: { typeAware: true },
  ignorePatterns: ["**/.hutch/**", "**/dist/**", "**/build/**"],
  rules: {
    // Its errors sit together, one module per concern.
    "max-classes-per-file": "off",
    // TypeScript reports real redeclarations; this flags a schema and its type sharing a name.
    "no-redeclare": "off",
    // Ported rule by rule from Switchboard's appendage: each step reads top to bottom as it did.
    "eslint/complexity": "off",
  },
})
