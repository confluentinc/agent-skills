# Sources vs streaming_source — decision rules

- **Use a dbt `source`** when: the topic exists already (whether produced by another team, by a connector you didn't manage from dbt, or by upstream pipelines). The adapter doesn't need to do anything; the topic is automatically a Flink table.
- **Use `streaming_source`** when: dbt should *create* the table with a connector. Today this primarily means Datagen-style synthetic data for testing/dev. The `connector` config is mandatory; the model body is column DDL.

If the user is doing both (production reads from a real topic, dev reads from synthetic data), the cleanest pattern is a dbt `source` for production targets and a `streaming_source` model for dev, selected by target.

See `../assets/sources.yml` for a copy-pasteable `source` declaration and `../assets/streaming_source.sql` for a connector-backed model.
