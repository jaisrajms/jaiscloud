#!/usr/bin/env python3
"""Enforce the console's pnpm-audit policy.

`pnpm audit --json` exits non-zero whenever it finds an advisory, so
`make security-scan-ui` captures its JSON and lets this script decide. Every
advisory must be cited in docs/security/pnpm-audit-allowlist.json, and every
allowlist entry must still match an advisory: an unlisted finding fails, and a
stale entry fails too (so a fixed advisory cannot stay ignored).

The audit JSON is pnpm 9's npm-v1 shape (`{"advisories": {...}}`). A future
pnpm that changes the shape fails loudly instead of silently passing.
"""
import json
import sys
from typing import NoReturn


def fail(message: str) -> NoReturn:
    print(f"security-scan-ui: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: pnpm_audit_allowlist.py <pnpm-audit.json> <allowlist.json>")
    audit_path, allow_path = sys.argv[1], sys.argv[2]

    try:
        with open(audit_path, encoding="utf-8") as fh:
            audit = json.load(fh)
    except OSError as exc:
        fail(f"cannot read the pnpm audit JSON ({audit_path}): {exc}")
    except json.JSONDecodeError as exc:
        fail(f"the pnpm audit output is not JSON ({audit_path}): {exc}")

    if "advisories" not in audit:
        fail(
            "unsupported pnpm audit JSON: no 'advisories' key "
            "(the pnpm version or output shape changed)"
        )

    try:
        with open(allow_path, encoding="utf-8") as fh:
            allow = json.load(fh)
    except OSError as exc:
        fail(f"cannot read the allowlist ({allow_path}): {exc}")
    except json.JSONDecodeError as exc:
        fail(f"the allowlist is not JSON ({allow_path}): {exc}")

    allowed = {}
    for entry in allow.get("advisories", []):
        advisory_id = entry.get("id")
        if not advisory_id:
            fail(f"allowlist entry without an id: {entry!r}")
        allowed[advisory_id] = entry

    seen = set()
    bad = False
    for key, advisory in (audit.get("advisories") or {}).items():
        advisory_id = advisory.get("github_advisory_id") or key
        seen.add(advisory_id)
        entry = allowed.get(advisory_id)
        if entry is None:
            print(
                "UNALLOWED advisory "
                f"{advisory_id} ({advisory.get('module_name')}, "
                f"{advisory.get('severity')}): {advisory.get('title')}\n"
                f"  cite it in {allow_path} or upgrade the dependency",
                file=sys.stderr,
            )
            bad = True
            continue
        if entry.get("package") and entry["package"] != advisory.get("module_name"):
            print(
                f"MISMATCH {advisory_id}: allowlist package "
                f"{entry['package']!r} != audited package "
                f"{advisory.get('module_name')!r}",
                file=sys.stderr,
            )
            bad = True

    for advisory_id in allowed:
        if advisory_id not in seen:
            print(
                f"STALE allowlist entry {advisory_id}: pnpm no longer reports it; "
                "remove the entry",
                file=sys.stderr,
            )
            bad = True

    if bad:
        raise SystemExit(1)
    print(
        f"security-scan-ui: {len(seen)} advisory(ies), all cited in {allow_path}"
    )


if __name__ == "__main__":
    main()
