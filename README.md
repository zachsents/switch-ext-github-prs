# switch-ext-github-prs

Each one's state and checks, within seconds.

A [Switch](https://switch.ax) extension, built on [switch-ext-kit](https://github.com/zachsents/switch-ext-kit). Its messages carry who sent them, for Switch's inbox and Contacts.

## Install

Ask Switch's agent to install `github:zachsents/switch-ext-github-prs`, or add it in the desktop's Extensions view (⌘K, Extensions). Switch runs it
on the first machine with what it needs (`always-on`).

## Setup

`gh auth login` on the machine that runs it.

## Develop

```sh
bun install
bun run check
bun start   # serves MCP on stdio
```
