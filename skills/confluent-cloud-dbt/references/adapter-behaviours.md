# dbt-confluent adapter — version-aware quirks & fixes

This file maps known adapter quirks to the version that **fixed** them, so you can tell whether a quirk still applies to the version the user is on. Most users arrive here from an *error message*, not a version number — the table is symptom-first.

> Latest known release: **0.2.1** (verified 2026-06-15). This marker goes stale — verify the live latest version at https://pypi.org/pypi/dbt-confluent/json (`.info.version`). Quirk/fix details live in the GitHub changelog: https://github.com/confluentinc/dbt-confluent/releases (issues: https://github.com/confluentinc/dbt-confluent/issues).

## How to use this file — run the currency check FIRST

Do these in order. Do **not** skip ahead to the quirk table.

1. **Find the version in use (`Vuser`).** Prefer the pin in `requirements.txt` / `pyproject.toml`. If unpinned, get the actually-installed version: `pip show dbt-confluent` or `dbt --version`.
2. **Determine the latest release.** WebFetch `https://pypi.org/pypi/dbt-confluent/json` and read `.info.version` (this is what `pip install` resolves to — same source the scaffold workflow uses to pin). If WebFetch is unavailable or denied, fall back to the "Latest known release" marker and **tell the user it may be stale**.
3. **Is `Vuser` out of date?**
   - **On the latest release** → there are no later fixes to apply. **Do not dump the table.** Only consult a specific row if the user reports a matching symptom (it's still a real, current behaviour for them).
   - **Behind the latest** → every row whose **Fixed in** is newer than `Vuser` is a quirk the user can still hit. Surface those proactively — and call one out immediately if the user reports its symptom.
4. **Only then suggest an upgrade**, naming the **minimal** target version that clears the user's relevant rows (not just "upgrade to latest"). If a row's Fixed-in version is imprecise (`0.2.x`), err toward surfacing it and confirm against the release notes.

## Quirks by fix version

| Symptom the user sees | Affected | Fixed in | Guidance |
|---|---|---|---|
| `read operation timed out` on the first run after a cold cluster — the unified drift-catalog `UNION ALL` (`INFORMATION_SCHEMA.COLUMNS` + `TABLES` + `TABLE_OPTIONS`) exceeded the old 5s `confluent-sql` HTTP timeout. | `< 0.2.1` | `0.2.1` | Default timeout is now 60s. Upgrade; or as a stopgap on an old pin, raise the HTTP timeout. |
| `403` (not `404`) on statement DELETE during `dbt run` cleanup, under a compute-pool-scoped FlinkDeveloper role that can't see the statement. | `< 0.2.x` *(confirm exact tag in release notes)* | `0.2.x` | Adapter now **warns** instead of erroring, treating the statement as already-gone. Fix is upgrading the adapter, **not** broadening RBAC. |
| `409` name-conflict on CREATE — statement DELETE returns synchronously but tear-down completes asynchronously, so a fast re-CREATE collides. | `< 0.2.x` *(confirm exact tag in release notes)* | `0.2.x` | Adapter now retries CREATE on a `409`. Upgrade. |

> Maintainers: when adding a row, fill in the exact **Fixed in** tag — the currency check's "newer than `Vuser`" comparison depends on it. Keep the table sorted by Fixed-in version; if it grows large, split into per-minor-version sections under this heading.
