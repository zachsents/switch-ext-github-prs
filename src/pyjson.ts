// Python's `json.dumps(value, sort_keys=True)`, byte for byte: what a record's id was hashed from
// before the port, so the same state still makes the same id.
const SHORT: Readonly<Record<string, string>> = {
  '"': '\\"',
  "\\": "\\\\",
  "\b": "\\b",
  "\f": "\\f",
  "\n": "\\n",
  "\r": "\\r",
  "\t": "\\t",
}

const PRINTABLE_FROM = 0x20
const PRINTABLE_TO = 0x7e
const ASTRAL = 0xff_ff
const PLANE = 0x1_00_00
const TEN_BITS = 0x4_00
const HIGH = 0xd8_00
const LOW = 0xdc_00

const unit = (n: number) => `\\u${n.toString(16).padStart(4, "0")}`

// ensure_ascii: everything outside space..~ as \uXXXX, by UTF-16 unit, as Python writes it.
const quote = (s: string) => {
  let out = '"'
  for (const ch of s) {
    const short = SHORT[ch]
    const point = ch.codePointAt(0) ?? 0
    if (short !== undefined) {
      out += short
    } else if (point > ASTRAL) {
      // As UTF-16 does: a surrogate pair.
      const v = point - PLANE
      out += `${unit(HIGH + Math.floor(v / TEN_BITS))}${unit(LOW + (v % TEN_BITS))}`
    } else if (point < PRINTABLE_FROM || point > PRINTABLE_TO) {
      out += unit(point)
    } else {
      out += ch
    }
  }
  return `${out}"`
}

const byCodePoint = (a: string, b: string) => {
  if (a < b) {
    return -1
  }
  return a > b ? 1 : 0
}

export const pyJson = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "null"
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false"
  }
  if (typeof value === "number") {
    return String(value)
  }
  if (typeof value === "string") {
    return quote(value)
  }
  if (Array.isArray(value)) {
    return `[${value.map(pyJson).join(", ")}]`
  }
  if (typeof value === "object") {
    const entries = Object.entries(value).toSorted(([a], [b]) => byCodePoint(a, b))
    return `{${entries.map(([k, v]) => `${quote(k)}: ${pyJson(v)}`).join(", ")}}`
  }
  return "null"
}
