---
name: upgrade-kafka-python-client
description: "Upgrade the confluent-kafka-python library version in a user's project. Use this skill whenever a user wants to update, upgrade, or bump their confluent-kafka-python dependency — whether they say 'update kafka python client', 'upgrade confluent-kafka', 'bump confluent-kafka version', 'migrate to latest confluent-kafka', or mention version compatibility issues with confluent-kafka-python. Also use it when the user mentions deprecation warnings or breaking changes from confluent-kafka-python after a version change."
---

# Upgrade confluent-kafka-python

Upgrade the `confluent-kafka-python` dependency in a user's project to a target version, handling dependency pin updates, breaking API changes, and verification.

The library lives at https://github.com/confluentinc/confluent-kafka-python and follows semantic versioning. Upgrades can range from simple patch bumps to major transitions that rename APIs and drop Python version support.

## Step 1: Assess the Current State

Before making any changes, gather information about the project:

1. **Find the current version pin.** Check these files (in order of likelihood):
   - `requirements.txt`
   - `pyproject.toml` (under `[project.dependencies]` or `[tool.poetry.dependencies]`)
   - `setup.py` / `setup.cfg`
   - `Pipfile`
   - `conda` environment files

2. **Identify how the library is used.** Search the codebase for:
   - `from confluent_kafka import` and `import confluent_kafka`
   - `from confluent_kafka.schema_registry import`
   - `from confluent_kafka.admin import`
   - `from confluent_kafka.aio import` (async API, added in v2.12.0)
   - `from confluent_kafka.schema_registry._async` (async serializers, added in v2.12.0)

3. **Determine the target version.** If the user specified a version, use that. Otherwise, look up the latest release:
   - Check PyPI: `pip index versions confluent-kafka` or fetch https://pypi.org/pypi/confluent-kafka/json
   - Or check the GitHub releases page

4. **Check Python version compatibility.** The library has dropped support for older Python versions over time:
   - v2.6.1+: requires Python >= 3.7 (dropped 3.6)
   - v2.12.1+: requires Python >= 3.8 (dropped 3.7)
   - Verify the user's project Python version is compatible with the target library version

Report findings to the user before proceeding. If there are potential breaking changes, outline them and ask for confirmation.

## Step 2: Understand Breaking Changes

These are the significant changes across versions that can break user code. Check which ones apply based on the current-to-target version range:

### v2.12.0 — AsyncIO Support and KIP-848
- Added `AIOProducer` and `AIOConsumer` in `confluent_kafka.aio`
- Added `AsyncAvroSerializer` / `AsyncAvroDeserializer` in `confluent_kafka.schema_registry._async.avro`
- Added `AsyncSchemaRegistryClient`
- New KIP-848 consumer group protocol (opt-in via `group.protocol=consumer`)
- These are additive changes — existing sync code is unaffected, but users may want to migrate to async

### v2.12.1 — Python 3.7 Dropped
- If the user's project supports Python 3.7, they need to either drop that support or pin to `<2.12.1`

### v2.13.0 — Type Hinting Enforcement
- All public interfaces now have enforced type hints
- Code that passes wrong types (e.g., `int` where `str` is expected in config values) will now raise `TypeError` at runtime instead of silently converting
- Context manager support added for Producer/Consumer — `async with AIOProducer(conf) as p:` works now
- Check for config values that might be passed as wrong types (common: passing `int` for `message.max.bytes` instead of the expected type)

### v2.14.0 — Latest
- Check the changelog for any additional changes: https://github.com/confluentinc/confluent-kafka-python/blob/master/CHANGELOG.md

### General Patterns to Watch For
- **Deprecated config keys**: Kafka client configs get renamed across librdkafka versions (which confluent-kafka-python bundles). Check for deprecation warnings.
- **Schema Registry URL config**: Older code might use `schema.registry.url` in the Kafka config dict instead of the separate `SchemaRegistryClient` — this pattern was deprecated long ago but some projects still use it.
- **Import path changes**: Some internal modules get reorganized between versions.

## Step 3: Update the Dependency Pin

Update the version in the appropriate dependency file:

- **requirements.txt**: Update the version specifier (e.g., `confluent-kafka[avro]==2.14.0` or `confluent-kafka>=2.14.0,<3.0`)
- **pyproject.toml**: Update under the relevant section
- **Pipfile**: Update in `[packages]`
- Keep the same extras the user already had (e.g., `[avro]`, `[schema_registry]`, `[json]`, `[protobuf]`)
- If the user had no extras but uses Schema Registry, suggest adding the appropriate extras

## Step 4: Fix Breaking Changes in Code

Apply targeted fixes based on the version gap identified in Step 2:

1. **Type hint enforcement (upgrading to >= 2.13.0)**: Search for config dictionaries and verify all values match expected types. Common fix: ensure config values like `message.max.bytes` are passed as the correct type.

2. **Deprecated imports**: Check if any imports reference internal/private modules that may have moved. Update import paths.

3. **API signature changes**: If function signatures changed, update call sites.

4. **Python version metadata**: If `pyproject.toml` or `setup.py` declares `python_requires` or classifiers, update them to reflect the new minimum Python version if applicable.

For each change, explain to the user what changed and why, so they understand the migration.

## Step 5: Verify the Upgrade

Run verification steps to confirm the upgrade works:

1. **Install the updated dependency:**
   ```
   pip install -r requirements.txt
   ```
   (Or the equivalent for pyproject.toml / Pipfile)

2. **Run existing tests:**
   ```
   pytest
   ```
   If tests fail, diagnose whether failures are due to the upgrade or pre-existing issues.

3. **Check for deprecation warnings:**
   ```
   python -W all -c "import confluent_kafka; print(confluent_kafka.version())"
   ```

4. **If no tests exist**, suggest the user do a quick smoke test — produce a message and consume it — to verify the upgrade didn't break their workflow.

5. **Report results** to the user: what version they're now on, what changed, and whether tests pass.

## Reference: Version History Quick Reference

| Version | Key Changes | Python Support |
|---------|------------|----------------|
| 2.14.0  | Latest stable | >= 3.8 |
| 2.13.0  | Type hints enforced, context managers | >= 3.8 |
| 2.12.1  | Dropped Python 3.7 | >= 3.8 |
| 2.12.0  | AsyncIO producer/consumer, KIP-848 | >= 3.7 |
| 2.6.1   | Dropped Python 3.6 | >= 3.7 |
| 2.6.0   | AdminClient improvements | >= 3.6 |

For the full changelog, see: https://github.com/confluentinc/confluent-kafka-python/blob/master/CHANGELOG.md
