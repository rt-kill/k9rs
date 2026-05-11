# k9rs

A Kubernetes TUI written in Rust, inspired by [k9s](https://k9scli.io/).

## Features

- Browse and manage Kubernetes resources (pods, deployments, services, nodes, CRDs, etc.)
- Real-time streaming with automatic refresh
- Describe, YAML view, edit, delete, scale, restart, port-forward, shell
- Log streaming with filtering, timestamps, and wrap mode
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

Press `?` in any view to see the full help overlay. Key highlights:

| Key | Action |
|-----|--------|
| `:` | Command mode (type resource names, `:ns`, `:ctx`) |
| `/` | Filter current view |
| `?` | Help |
| `d` | Describe |
| `y` | YAML view |
| `e` | Edit |
| `Enter` | Drill down |
| `Esc` | Back / clear filter |
| `q` | Quit (or back if drilled) |
| `Ctrl-c` | Quit |

Resource-specific keys (shown in `?` help when available):
- `Shift-L` — Logs
- `s` — Shell / Scale (context-dependent)
- `r` — Restart
- `Shift-F` — Port forward
- `o` — Show node

## Configuration

Config file: `~/.config/k9rs/config.yaml`

```yaml
k9rs:
  # TUI preferences
  ui:
    skin: dracula              # skin name (loads from skins/ directory)
    pageScrollLines: 40        # lines per PageUp/PageDown
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
      tailLines: 500           # lines fetched on log open
      defaultFollow: true      # auto-scroll logs
      defaultTimestamps: true  # show timestamps
      defaultWrap: false       # wrap long lines

  # Daemon tuning
  daemon:
    watcherPageSize: 1000      # K8s LIST pagination batch
    discoveryRefreshSecs: 300  # CRD/namespace discovery poll interval
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
