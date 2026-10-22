#!/usr/bin/env python3
"""Cross-module coupling matrix — the checkpoint oracle for the crate split.

Counts `crate::<module>` references, grouped by the referring file's own
top-level module, so "did coupling actually drop" is a number rather than an
opinion. Run it before and after any re-filing step.

    python3 tools/coupling.py            # source only (src/, excluding src/tests/)
    python3 tools/coupling.py --tests    # test mirrors too, listed separately
    python3 tools/coupling.py --proto    # can k9rs-proto be extracted yet?

A cycle is any pair with references in both directions. Crate boundaries
cannot be drawn through one, because Rust forbids circular crate
dependencies — so the cycle count reaching zero is what turns the split from
a redesign into a Cargo.toml change.
"""
import collections
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent / "src"


def modules():
    """Top-level modules, discovered rather than hardcoded so a new one
    cannot be silently left out of the matrix."""
    found = {p.name for p in ROOT.iterdir() if p.is_dir() and p.name != "tests"}
    found |= {p.stem for p in ROOT.glob("*.rs") if p.stem != "main"}
    return sorted(found)


def owner(path, mods):
    """Which module a file belongs to. Test mirrors are attributed to the
    module they mirror, since that is the crate they will compile into."""
    parts = path.relative_to(ROOT).parts
    if parts[0] == "tests":
        return parts[1] if len(parts) > 2 and parts[1] in mods else (
            pathlib.Path(parts[1]).stem if len(parts) > 1 else "main")
    return parts[0] if parts[0] in mods else pathlib.Path(parts[0]).stem


def scan(include_tests):
    mods = modules()
    edges = collections.Counter()
    per_file = collections.defaultdict(collections.Counter)
    for path in ROOT.rglob("*.rs"):
        is_test = path.relative_to(ROOT).parts[0] == "tests"
        if is_test and not include_tests:
            continue
        src = owner(path, mods)
        text = path.read_text()
        for m in mods:
            n = len(re.findall(r"crate::" + re.escape(m) + r"\b", text))
            if n and m != src:
                edges[(src, m)] += n
                per_file[(src, m)][str(path.relative_to(ROOT))] += n
    return mods, edges, per_file


# The prospective `k9rs-proto` file set: the wire contract plus the resource
# DEFINITIONS. A crate boundary here is only real if nothing REACHABLE from
# these files names kube-rs — a file-level grep misses that, because the
# dependency used to run through a function call (`protocol.rs` -> `REGISTRY`).
PROTO_SEED = [
    "kube/protocol.rs", "kube/resource_def/mod.rs", "kube/resources/row.rs",
    "kube/local/types.rs", "kube/overlay.rs",
]
HEAVY = ("kube", "k8s_openapi")


def _targets(path):
    """`crate::a::b::C` -> the file(s) that could define `a::b`."""
    parts = path.split("::")
    for n in range(len(parts), 0, -1):
        base = "/".join(parts[:n])
        hits = [c for c in (f"{base}.rs", f"{base}/mod.rs") if (ROOT / c).exists()]
        if hits:
            return hits
    return []


def proto_closure():
    seed = list(PROTO_SEED) + [
        f"kube/resource_def/{p.stem}.rs"
        for p in (ROOT / "kube/resource_def").glob("*.rs") if p.stem != "mod"
    ]
    seen, frontier, heavy = set(seed), list(seed), {}
    while frontier:
        f = frontier.pop()
        text = (ROOT / f).read_text()
        code = "\n".join(l for l in text.splitlines()
                         if not l.strip().startswith(("///", "//!", "//")))
        for h in HEAVY:
            if re.search(r"(?<![:a-z_])" + h + r"::", code):
                heavy.setdefault(f, []).append(h)
        for m in re.finditer(r"crate::([a-z_]+(?:::[a-z_]+)*)", code):
            for cand in _targets(m.group(1)):
                if cand not in seen:
                    seen.add(cand)
                    frontier.append(cand)
    return seen, heavy


def main():
    if "--proto" in sys.argv:
        seen, heavy = proto_closure()
        print(f"=== k9rs-proto extractability: {len(seen)} files reachable from the seed ===")
        if heavy:
            print("BLOCKED — these reachable files name kube-rs / k8s-openapi:")
            for f, hs in sorted(heavy.items()):
                print(f"  {f}: {', '.join(hs)}")
        else:
            print("CLEAR — nothing reachable names kube-rs or k8s-openapi")
        return
    include_tests = "--tests" in sys.argv
    mods, edges, per_file = scan(include_tests)
    label = "src + tests" if include_tests else "src only"

    print(f"=== coupling matrix ({label}): rows refer to columns ===")
    print("            " + "".join(f"{m:>9}" for m in mods))
    for a in ["main"] + mods:
        row = f"{a:<12}"
        for b in mods:
            v = edges.get((a, b), 0)
            row += f"{v:>9}" if v else f"{'.':>9}"
        print(row)

    cycles = sorted({tuple(sorted((a, b))) for (a, b) in edges
                     if (b, a) in edges and a in mods and b in mods})
    print(f"\n=== cycles: {len(cycles)} ===")
    for a, b in cycles:
        print(f"  {a} <-> {b}   ({a}->{b}: {edges[(a,b)]}, {b}->{a}: {edges[(b,a)]})")
    if not cycles:
        print("  none — every pair is one-directional")

    # The files carrying each upward edge, which is what a re-filing step moves.
    print("\n=== heaviest edges, by file ===")
    for (a, b), n in edges.most_common(6):
        print(f"  {a} -> {b}  ({n})")
        for f, c in per_file[(a, b)].most_common(4):
            print(f"      {c:>5}  {f}")


if __name__ == "__main__":
    main()
