# Best Practices for Confluent Skills

## Security and credentials

- **Never read `.env` files** — do not use cat, Read, head, grep, or any tool to view `.env` contents. Only reference environment variable names, never their values
- When verifying credentials are configured, check existence (`test -n "$VAR"`), not values
- Never hardcode credentials in skill instructions, test cases, or eval prompts
- Always use .env files for credential storage
- Remind users to add .env to .gitignore
- Use separate API keys for different environments
- Generated skills must include the same `.env` read prohibition in their instructions

## PII and synthetic data

**Skill files must never contain real customer data or secrets.** Everything committed into a skill — `SKILL.md`, `references/`, eval prompts in `evals.json`, on-disk fixtures (`mock-repos/`, `mock-skills/`, sample records), and bundled scripts — must use *synthetic* data only. This is the same rule the `confluent-skill-reviewer` enforces with `scripts/scan_pii.py`; a review will flag violations, so get it right at authoring time.

**Never embed (blocking — remove and rotate if ever real):**
- US Social Security Numbers or other government IDs
- Luhn-valid payment card numbers — use a clearly fake, non-Luhn placeholder
- AWS access key ids (`AKIA…`/`ASIA…`), private key blocks (`-----BEGIN … PRIVATE KEY-----`), or any live secret

**Avoid (use synthetic equivalents instead):**
- Real email addresses — use `example.com`, a `.example`/`.invalid`/`.test` domain, or an obvious placeholder like `you@example.com`
- Real phone numbers — use a fictional `555-01xx` number
- Real names, account ids, org ids, cluster ids, or hostnames pulled from an actual environment — replace with obvious placeholders

**Verify before shipping:** you can run the reviewer's scanner against the skill directory to catch anything that slipped in:

```bash
python3 skills/confluent-skill-reviewer/scripts/scan_pii.py path/to/created-skill
```

Generated skills should carry this same "synthetic data only" guidance so their authors and evals stay clean.

## Error handling

- Check if resources exist before trying to create them
- Provide helpful error messages (e.g., "Topic 'orders' not found. Create it in Confluent Cloud UI first.")
- Handle Schema Registry compatibility errors gracefully
- Fail loudly with specific errors — never silently skip or produce empty output

## Documentation

- Link to official Confluent docs (docs.confluent.io), not outdated blog posts
- Include version compatibility notes (e.g., "Requires Flink 1.18+")
- Reference relevant Confluent examples from GitHub

## Testing

- Test against real Confluent environments, not mocks
- Verify actual data flow, not just API responses
- Test error cases (missing topics, schema mismatches, etc.)

## Platform-specific considerations

- **Confluent Cloud**: Topics must be pre-created; use Cloud API keys for resource management
- **Confluent Platform**: Security config varies (SASL, mTLS, Kerberos); ask user's auth method
- **Apache Kafka (OSS)**: No managed features (no Tableflow, no managed Flink); adjust scope accordingly
- **WarpStream**: Disable idempotence, increase batch sizes, large fetch sizes; see [WarpStream client config docs](https://docs.warpstream.com/warpstream/kafka/configure-kafka-client/tuning-for-performance.md)

## Bundled scripts

Depending on the skill's use case, include relevant scripts from this skill's `scripts/` directory:

- `check_compute_pool.py` — Flink skills
- `produce_data.py` — Producer/streaming skills (JSON_SR default)
- `consume_and_verify.py` — Consumer/verification skills
- `register_schema.py` — Schema Registry skills
- `cleanup_resources.py` — Clean up topics, schemas, Flink statements
