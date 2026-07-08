---
name: kafkajs-migration
description: Migrate Kafka applications from KafkaJS to Confluent's @confluentinc/kafka-javascript client. Use when asked to migrate, convert, or upgrade KafkaJS code. Trigger on KafkaJS migration, switch from kafkajs, replace kafkajs, upgrade kafkajs, move to confluent javascript client. Do NOT trigger for new Kafka projects from scratch, node-rdkafka migration, general Node.js questions, Kafka Streams, or building producers/consumers without an existing KafkaJS codebase.
---

# KafkaJS to confluent-kafka-javascript Migration Skill

Migrate Node.js Kafka applications from the unmaintained [KafkaJS](https://kafka.js.org/) library to Confluent's officially supported [`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript) client.

## Context Documents

- **[migration.md](https://docs.confluent.io/kafka-clients/javascript/current/migration.md)** — Official migration guide (the authoritative API reference for all config and method changes). Fetch this URL and use it as the primary reference.
- **`references/performance-guidance.md`** — Production-tested performance tuning for producer batching, consumer configuration, memory management, Kubernetes auto-scaling, and observability

## When to Use

Invoke with `/kafkajs-migration [path]` when:
- Migrating a file, module, or entire project from KafkaJS to confluent-kafka-javascript
- Reviewing code for KafkaJS patterns that need updating
- Understanding API differences between the two libraries

## Arguments

| Argument | Required | Description |
| --- | --- | --- |
| `path` | No | File or directory to migrate. If omitted, scans the current working directory for KafkaJS usage. |

## Workflow

### Phase 1: Discovery

1. **Check Node.js version**: Run `node -v` and check the `engines` field in the `@confluentinc/kafka-javascript` package on npm (`npm view @confluentinc/kafka-javascript engines`). Unlike KafkaJS (pure JavaScript), this package ships prebuilt native binaries and only works on Node versions it has binaries for. If the user's Node version is not listed, warn them before proceeding and suggest switching to a supported version.
2. If a path is provided, scan it. Otherwise scan the working directory.
3. Find all files importing `kafkajs`:
   ```bash
   grep -rn "require('kafkajs')\|from 'kafkajs'\|require(\"kafkajs\")\|from \"kafkajs\"" --include="*.js" --include="*.ts" --include="*.mjs" --include="*.cjs" .
   ```
4. Check `package.json` for the `kafkajs` dependency.
5. Present a summary: number of files, which patterns are used (producer, consumer, admin, transactions, schema registry).

### Phase 2: Migration

Fetch the [migration guide](https://docs.confluent.io/kafka-clients/javascript/current/migration.md) before starting any migrations. Use it as a checklist — validate every line of Kafka-related code against the documented changes. Specifically check for:

- **Config wrapping**: all KafkaJS config objects (client, producer, consumer, admin) must be nested inside a `kafkaJS` property.
- **Moved properties**: properties that changed location (e.g., `acks`/`compression`/`timeout` from `send()` to producer init, `fromBeginning` from `subscribe()` to consumer init, `autoCommit`/`autoCommitInterval` from `run()` to consumer init).
- **Removed method arguments**: options that are no longer supported (e.g., `waitForLeaders` in `createTopics`, `validateOnly`, `replicaAssignment`).
- **Removed method calls**: KafkaJS methods and patterns that don't exist in the Confluent client, including `producer.on()`/`consumer.on()` event listeners, manual `heartbeat()` calls inside `eachMessage`/`eachBatch` callbacks, and `consumer.stop()` (use `disconnect()` instead).
- **Changed error handling**: `instanceof` / `error.name` checks must switch to `Kafka.isKafkaJSError()` / `error.code`.

Do not limit changes to just import statements and config wrapping.

Migrate one file at a time. For each file:

1. **Show the proposed diff** — present the changes in diff format so the user can review before applying.
2. **Explain each change** with:
   - **Why**: 1-2 sentences on why this change is required (e.g., "The confluent-kafka-javascript client requires all KafkaJS config to be nested inside a `kafkaJS` property.").
   - **What to know**: 1-2 sentences highlighting any behavior changes the user should be aware of (e.g., "The default `acks` value changes from `undefined` (KafkaJS) to `-1` (all replicas). This means produces may be slightly slower but are more durable by default.").
3. **Wait for user confirmation** before applying changes and moving to the next file.

If a change has no meaningful behavior difference, the "What to know" can simply say "No behavior change — this is a structural migration only."

### Phase 3: Package Update

After all files are migrated, run the package swap:

```bash
npm uninstall kafkajs && npm install @confluentinc/kafka-javascript
```

Verify the install succeeded — check that `@confluentinc/kafka-javascript` appears in `package.json` dependencies and `kafkajs` is removed. If `@kafkajs/confluent-schema-registry` is present, leave it — it remains compatible.

### Phase 4: Verification

1. Run TypeScript compilation if applicable: `npx tsc --noEmit`
2. Run the project's test suite (e.g., `npm test`). If tests require environment variables (like `KAFKA_BROKERS`), check for a `.env` file, README, or test scripts to determine the correct invocation.
3. Report results to the user — pass/fail, any errors, and suggested fixes for failures.

---

## Quick Reference

The three most common migration patterns. For the full API reference (config tables, error types, admin client, transactions, auth), see the [migration guide](https://docs.confluent.io/kafka-clients/javascript/current/migration.md).

### 1. Import + Config Wrapping

```diff
-const { Kafka } = require('kafkajs');
+const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

 const kafka = new Kafka({
+  kafkaJS: {
     clientId: 'my-app',
     brokers: ['kafka1:9092', 'kafka2:9092'],
+  }
 });
```

All KafkaJS config nests inside a `kafkaJS` property. librdkafka properties (e.g., `ssl.ca.location`, `session.timeout.ms`) go outside that block. `brokers` must be a string array — async functions are no longer accepted.

### 2. Producer — `acks`/`compression`/`timeout` Move to Init

```diff
-const producer = kafka.producer();
+const producer = kafka.producer({
+  kafkaJS: { acks: 1, compression: 'GZIP', timeout: 30000 }
+});
 await producer.connect();
 await producer.send({
   topic: 'test',
   messages: [{ value: 'Hello' }],
-  acks: 1,
-  compression: CompressionTypes.GZIP,
-  timeout: 30000,
 });
```

### 3. Consumer — `fromBeginning`/`autoCommit` Move to Init

```diff
-const consumer = kafka.consumer({ groupId: 'my-group' });
+const consumer = kafka.consumer({
+  kafkaJS: { groupId: 'my-group', fromBeginning: true, autoCommit: true, autoCommitInterval: 5000 }
+});
 await consumer.connect();
-await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });
+await consumer.subscribe({ topic: 'test-topic' });
 await consumer.run({
-  autoCommit: true,
-  autoCommitInterval: 5000,
   eachMessage: async ({ topic, partition, message }) => {
     console.log(message.value.toString());
   },
 });
```

### Other Breaking Changes (see [migration.md](https://docs.confluent.io/kafka-clients/javascript/current/migration.md) for details)

- **SSL certs**: `ssl` is boolean-only in `kafkaJS` block; use librdkafka props (`ssl.ca.location`, etc.) for cert paths
- **OAUTHBEARER**: provider must return `lifetime` and `principal` in addition to `value`
- **Error handling**: use `Kafka.isKafkaJSError(error)` + `error.code` instead of `instanceof` / `error.name`
- **Transactions**: `sendOffsets` takes `consumer` object, not `consumerGroupId`
- **`consumer.stop()`**: removed — use `disconnect()` instead
- **`heartbeat()`**: automatic — remove manual calls
- **`eachBatch`**: max size defaults to 32 (configurable via `js.consumer.max.batch.size`)
- **Unsupported**: `socketFactory`, custom partitioners, `producer.on()`/`consumer.on()` events, `autoCommitThreshold`, `retry.restartOnFailure`
- **Schema Registry**: `@kafkajs/confluent-schema-registry` works unchanged

---

## Performance Guidance

For high-throughput applications, read `references/performance-guidance.md` before completing the migration. It covers producer batching patterns, consumer tuning, memory management (librdkafka fragmentation vs KafkaJS), Kubernetes auto-scaling for Node.js, metrics/observability differences, and known community issues.

---

## Key Sources

- [Migration guide (migration.md)](https://docs.confluent.io/kafka-clients/javascript/current/migration.md)
- [GitHub repo](https://github.com/confluentinc/confluent-kafka-javascript)
- [npm package](https://www.npmjs.com/package/@confluentinc/kafka-javascript)
- [librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
