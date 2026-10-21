# k9rs

A Kubernetes TUI written in Rust, inspired by [k9s](https://k9scli.io/).

## Features

- Browse and manage Kubernetes resources (pods, deployments, services, nodes, CRDs, etc.)
- Real-time streaming with automatic refresh
- Describe, YAML view, edit, delete, scale, restart, port-forward, shell
- Log streaming with filtering, timestamps, wrap mode and horizontal panning
- Drill-down navigation (deployment → pods → containers → logs)
- Context and namespace switching
- k9s-compatible skin/theme support
- User-defined overlays (custom columns, coloring rules, key bindings)
- Configurable TUI preferences and daemon tuning

## Architecture

k9rs uses a client/daemon architecture:

- **TUI** — renders the interface, handles user input, navigates resources
- **Daemon** — manages Kubernetes API connections, watches resources, streams data over a Unix socket

The TUI never talks to the Kubernetes cluster directly. All data flows through the daemon via a binary protocol (length-prefixed bincode over yamux-multiplexed Unix socket).

## Installation

```bash
cargo build --release
cp target/release/k9rs ~/.local/bin/
```

## Usage

```bash
k9rs                    # start with overview
k9rs pods               # start on pods
k9rs -n kube-system     # start in a specific namespace
k9rs --context prod     # start with a specific context
k9rs --readonly         # read-only mode (no mutations)
```

## Key Bindings

Press `?` in any view for the full keybinding table — a filterable, sortable
view of the keys for the view you pressed it in. Key highlights:

| Key | Action |
|-----|--------|
| `:` | Command mode (type resource names, `:ns`, `:ctx`) |
| `/` | Filter current view |
| `?` | Help |
| `Ctrl-a` | Aliases — everything `:` accepts (`:alias` does the same) |
| `d` | Describe |
| `y` | YAML view |
| `e` | Edit |
| `Enter` | Drill down |
| `Esc` | Back / clear filter |
| `q` | Quit (or back if drilled) |
| `Ctrl-c` | Quit |

Resource-specific keys (shown in `?` help when available):
- `l` — Logs
- `s` — Shell / Scale (context-dependent)
- `r` — Restart
- `Shift-F` — Port forward
- `o` — Show node

In a **log view** (`l`):

| Key | Action |
|-----|--------|
| `s` | Toggle autoscroll |
| `w` | Toggle line wrap |
| `t` | Cycle shown fields — timestamp + source + message, source + message, message |
| `h` / `l` | Pan one column left / right |
| `H` / `L` | Pan a stride left / right |
| `^` / `$` | Pan to the left margin / the far edge |
| `/` | Grep (stacks — each commit narrows the last) |
| `/!pat` | Grep the other way: EXCLUDE lines matching `pat` (`\!` for a literal `!`) |
| `0`–`6` | Time range: tail, 1m, 5m, 15m, 30m, 1h, 24h |
| `7` | Everything, oldest first — stops once the buffer is full |
| `Shift-C` | Clear the buffer |
| `Ctrl-s` | Save to a file |

Marking rows with `Space` enters **select mode**: single-row keys go
dead, and `Ctrl-d` / `r` / `Ctrl-k` act on ALL marked rows (with a
confirm dialog listing the targets). `Ctrl-\` clears the selection.

## Configuration

Config file: `~/.config/k9rs/config.yaml`

```yaml
k9rs:
  noExitOnCtrlC: false       # Ctrl-C quits (it is a key, not a signal —
                             #   the TUI runs in raw mode)
  readOnly: false            # refuse every mutation client-side as well as
                             #   server-side

  # Key rebindings. An operation entry MOVES that operation's binding (its
  # default key stops working) — except for delete, forceKill and portForward,
  # whose real chords (Ctrl-D / Ctrl-K / Shift-F) are structural, so rebinding
  # those ADDS a second key rather than moving the first. The col* entries ADD
  # chords for column
  # movement (arrows always work) and win over an operation bound to the
  # same key. In a LOG view the same col* chords pan horizontally — one
  # "move left/right in this view" intent, answered by each view in its own
  # units. Chords: "x", "X", or "ctrl-x".
  # Rebindable operations: describe, yaml, logs, previousLogs, shell,
  # restart, scale, delete, forceKill, portForward, showNode, nodeShell,
  # decodeSecret, triggerCronJob, toggleSuspendCronJob.
  keys:
    colLeft: h                 # vim-style column movement …
    colRight: l
    colFirst: "0"              # … and jumps to the first / last column
    colLast: "$"               #   (in a log view: the pan margins)
    namespaceAll: ")"          # rebind all-namespaces, freeing `0`
    logs: ctrl-l               # … freed by moving logs off plain `l`

  # TUI preferences
  ui:
    theme: auto                # auto | dark | light — `auto` asks the
                               #   terminal for its background colour and
                               #   falls back to dark. `--theme <mode>`
                               #   overrides this for one run.
    skin: dracula              # skin name (loads from skins/ directory);
                               #   layers on top of the chosen theme
    maxColumnWidth: 64         # max column width before truncation
    commandHistorySize: 50     # max : command history entries
    changeHighlightSecs: 5     # row change highlight duration
    cacheCapacity: 100         # describe/yaml cache entries
    searchContextLines: 10     # lines above search match
    flash:
      infoSecs: 3              # info message duration
      warnSecs: 5              # warning message duration
      errorSecs: 10            # error message duration
    logs:
      maxLines: 50000          # log buffer size
      tailLines: 100           # lines fetched on log open
      defaultFollow: true      # auto-scroll logs
      defaultWrap: false       # wrap long lines (`w`)
      defaultFields: source    # per-line fields to start on; `t` cycles them.
                               #   all     = timestamp + source tag + message
                               #   source  = source tag + message
                               #   message = message only

  # Daemon tuning
  daemon:
    watcherPageSize: 1000      # K8s LIST pagination batch
    discoveryRefreshSecs: 300  # CRD/namespace discovery poll interval
    fanoutRing: 64             # delta batches buffered per watcher
    watcherGraceSecs: 60       # keep a watcher warm this long after the last viewer leaves
    localsGraceSecs: 1800      # keep a context's PORT-FORWARDS alive this long after its
                               # last session detaches. Deliberately much longer than
                               # watcherGraceSecs: a warm watcher is a cache (costs a row
                               # store + an apiserver watch, buys back a re-LIST), whereas a
                               # port-forward is something you asked for and would have to
                               # notice dying and recreate.
    backoff:
      initialMs: 300           # watcher retry start
      maxMs: 30000             # max single retry sleep
      maxElapsedMs: 120000     # total retry timeout
    execResources:             # user-defined local resources backed by commands
      - name: pod-metrics      # unique name (navigate via :pod-metrics)
        command: kubectl
        args: ["top", "pods", "-A", "--no-headers"]
        headers: ["NAMESPACE", "NAME", "CPU", "MEMORY"]
        jsonFieldKeys: []      # empty = raw TSV parsing
        pollIntervalSecs: 30
```

All fields are optional — omitting them uses the defaults shown above.
Typos in field names produce an error (strict validation via `deny_unknown_fields`).

## Overlays

User-defined resource customizations. Place YAML files in `~/.config/k9rs/overlays/`.

```yaml
# ~/.config/k9rs/overlays/nodeclaims.yaml
resource: nodeclaims

# Named capabilities with typed implementations
capabilities:
  show-node:
    type: drill
    target: nodes
    column: NODE

# Key → capability name bindings
bindings:
  o: show-node

# Extra columns (JSONPath for CRDs, level override for built-ins)
columns:
  - header: "INSTANCE TYPE"
    jsonpath: ".spec.instanceType"
  - header: "NODE"
    jsonpath: ".status.nodeName"

# Declarative coloring rules
coloring:
  - column: "STATUS"
    rules:
      - match: "NotReady"
        health: failed
      - match: "Pending"
        health: pending
  - column: "CPU%"
    rules:
      - when: ">= 90"
        health: failed
      - when: ">= 70"
        health: pending
```

### Overlay Features

**Coloring rules** — map cell values to row health (Normal/Pending/Failed → green/yellow/red). Works for both CRDs and built-in resources.

**Extra columns** — JSONPath extraction for CRDs. For built-in resources, use `level: default` to promote existing extra columns to default visibility.

**Key bindings** — bind keys to named capabilities. Built-in keys can't be shadowed. Capabilities define the implementation (e.g., drill to another resource filtered by a column value).

## Themes

k9s-compatible skin files. Place in `~/.config/k9rs/skins/` and reference by name in config.

```yaml
# ~/.config/k9rs/config.yaml
k9rs:
  ui:
    skin: dracula
```

The skin file format matches [k9s skin YAML](https://k9scli.io/topics/skins/).

## License

MIT
