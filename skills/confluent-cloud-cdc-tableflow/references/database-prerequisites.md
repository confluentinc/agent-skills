# Database Prerequisites for CDC

Each database requires specific configuration to support Change Data Capture. This guide covers the prerequisites for each supported database type.

## PostgreSQL CDC Prerequisites

### Required Configuration (postgresql.conf, restart required)

```
wal_level = logical
max_replication_slots = 4    # at least 1 per connector
max_wal_senders = 4          # at least 1 per connector
```

### Required Permissions & Publication

```sql
ALTER USER <connector_user> WITH REPLICATION;
GRANT CONNECT ON DATABASE <database> TO <connector_user>;
GRANT USAGE ON SCHEMA <schema> TO <connector_user>;
GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO <connector_user>;
ALTER DEFAULT PRIVILEGES IN SCHEMA <schema> GRANT SELECT ON TABLES TO <connector_user>;

CREATE PUBLICATION dbz_publication FOR TABLE table1, table2, table3;
```

### Cloud-Specific Notes

**AWS RDS/Aurora:**
- Set `rds.logical_replication = 1` in parameter group
- Reboot instance after parameter change
- Use the master user or create user with `rds_replication` role

**Google Cloud SQL:**
- Set `cloudsql.logical_decoding = on` in flags
- Restart instance
- Grant `cloudsqlsuperuser` role to connector user

**Azure Database for PostgreSQL:**
- Set `wal_level = logical` in server parameters
- Set `max_replication_slots >= 4`
- Restart server

### Replication Slot Cleanup

**Critical:** When a CDC connector is deleted, its named replication slot is NOT automatically dropped. Orphaned replication slots hold WAL segments indefinitely, causing disk usage to grow until the database runs out of storage.

**After deleting a connector**, always clean up the replication slot:

```sql
-- List active replication slots
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;

-- Drop the orphaned slot (only when 'active' = false)
SELECT pg_drop_replication_slot('debezium_slot');
```

**Monitor for orphaned slots** — set up an alert if `pg_wal_lsn_diff` grows beyond a threshold (e.g., 1 GB) for any inactive slot. On managed services like RDS, orphaned slots can fill the allocated storage and cause the instance to become read-only.

### Verification

```sql
SHOW wal_level;                          -- must be 'logical'
SHOW max_replication_slots;              -- must be >= 1
SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication';
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal FROM pg_replication_slots;
```

### Documentation

- **Confluent PostgreSQL CDC Source V2 connector:** https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html
- **Confluent PostgreSQL CDC prerequisites:** https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/prereqs-validation.html
- **Debezium PostgreSQL connector:** https://debezium.io/documentation/reference/2.4/connectors/postgresql.html
- **AWS RDS PostgreSQL:** https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html

---

## MySQL CDC Prerequisites

### Required Configuration (my.cnf, restart required)

```
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
gtid_mode = ON                       # recommended
enforce_gtid_consistency = ON        # recommended
binlog_expire_logs_seconds = 604800  # 7 days minimum
```

### Required Permissions

```sql
CREATE USER '<connector_user>'@'%' IDENTIFIED BY '<password>';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '<connector_user>'@'%';
GRANT LOCK TABLES ON <database>.* TO '<connector_user>'@'%';
GRANT SELECT ON <database>.* TO '<connector_user>'@'%';
FLUSH PRIVILEGES;
```

**LOCK TABLES on managed MySQL:** Managed services (RDS, Aurora, Cloud SQL, Azure) don't grant `SUPER`, so Debezium can't use `FLUSH TABLES WITH READ LOCK` and falls back to per-table `LOCK TABLES`. Without this grant, the connector fails during snapshot.

### Cloud-Specific Notes

**AWS RDS/Aurora:**
- Binary logging is enabled by default
- Set `binlog_format = ROW` in parameter group
- Enable automated backups (required for binlog)
- User needs `mysql.rds_set_configuration` for binlog retention
- **Grant `LOCK TABLES`** — RDS does not grant `SUPER`, so Debezium needs per-table locks for snapshots

**Google Cloud SQL:**
- Enable binary logging in flags
- Set `binlog_format = ROW`
- Set retention period in days
- **Grant `LOCK TABLES`** — Cloud SQL restricts `SUPER`, same as RDS

**Azure Database for MySQL:**
- Enable binary logging in server parameters
- Set `binlog_format = ROW`
- Configure binlog retention
- **Grant `LOCK TABLES`** — Azure restricts `SUPER`, same as RDS

### Verification

```sql
SHOW VARIABLES LIKE 'log_bin';           -- must be ON
SHOW VARIABLES LIKE 'binlog_format';     -- must be ROW
SHOW VARIABLES LIKE 'gtid_mode';         -- should be ON
SHOW GRANTS FOR '<connector_user>'@'%';
```

### Documentation

- **Confluent MySQL CDC Source V2 connector:** https://docs.confluent.io/cloud/current/connectors/cc-mysql-cdc-source-v2-debezium/cc-mysql-cdc-source-v2-debezium.html
- **Confluent MySQL CDC prerequisites:** https://docs.confluent.io/cloud/current/connectors/cc-mysql-cdc-source-v2-debezium/prereqs-validation.html
- **Debezium MySQL connector:** https://debezium.io/documentation/reference/2.4/connectors/mysql.html
- **AWS RDS MySQL:** https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_MySQL.html

---

## SQL Server CDC Prerequisites

### Enable CDC and Permissions

**Critical:** SQL Server Agent must be running for CDC to work.

```sql
-- Enable CDC on database
USE <database_name>;
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on each table
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'<table_name>', @role_name = NULL, @supports_net_changes = 1;

-- Create connector user with permissions
CREATE LOGIN <connector_user> WITH PASSWORD = '<password>';
CREATE USER <connector_user> FOR LOGIN <connector_user>;
GRANT SELECT ON SCHEMA :: dbo TO <connector_user>;
GRANT SELECT ON SCHEMA :: cdc TO <connector_user>;
GRANT VIEW DATABASE STATE TO <connector_user>;
EXEC sp_addrolemember N'db_datareader', N'<connector_user>';
```

### Cloud-Specific Notes

**AWS RDS SQL Server:**
- Use `msdb.dbo.rds_cdc_enable_db` instead of `sys.sp_cdc_enable_db`
- SQL Server Agent runs automatically
- Example:
  ```sql
  EXEC msdb.dbo.rds_cdc_enable_db '<database_name>';
  ```

**Azure SQL Database:**
- CDC is supported on all service tiers
- SQL Server Agent equivalent runs automatically
- Use standard CDC procedures

### Verification

```sql
SELECT name, is_cdc_enabled FROM sys.databases;
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE is_tracked_by_cdc = 1;
EXEC sys.sp_cdc_help_jobs;
```

### Documentation

- **Confluent SQL Server CDC Source V2 connector:** https://docs.confluent.io/cloud/current/connectors/cc-sqlserver-cdc-source-v2-debezium/cc-sqlserver-cdc-source-v2-debezium.html
- **Confluent SQL Server CDC prerequisites:** https://docs.confluent.io/cloud/current/connectors/cc-sqlserver-cdc-source-v2-debezium/prereqs-validation.html
- **Debezium SQL Server connector:** https://debezium.io/documentation/reference/2.4/connectors/sqlserver.html
- **AWS RDS SQL Server:** https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SQLServer.html
- **RDS SQL Server CDC:** https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.SQLServer.CommonDBATasks.CDC.html

---

## Oracle XStream CDC Prerequisites

**Connector class:** `OracleXStreamSource`

**Supported platforms:**
- Self-managed Oracle Database 19c+
- Amazon RDS for Oracle (non-CDB architecture only)

**NOT supported:**
- Oracle Autonomous Databases
- Oracle Standby databases (Data Guard)
- Downstream Capture configurations
- CDB (Container Database) architecture on RDS

**Licensing:** Requires a valid Confluent license for Oracle XStream Out.

### Documentation

- **Confluent Oracle XStream CDC Source connector:** https://docs.confluent.io/cloud/current/connectors/cc-oracle-xstream-cdc-source/cc-oracle-xstream-cdc-source.html
- **Confluent Oracle XStream CDC prerequisites:** https://docs.confluent.io/cloud/current/connectors/cc-oracle-xstream-cdc-source/prereqs-validation.html
- **Working with Amazon RDS for Oracle:** https://docs.confluent.io/cloud/current/connectors/cc-oracle-xstream-cdc-source/prereqs-validation.html#working-with-amazon-rds-for-oracle
- **AWS RDS Oracle:** https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_Oracle.html
- **Oracle XStream concepts:** https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html

### Step 1: Enable GoldenGate Replication

XStream requires GoldenGate replication to be enabled at the database level.

**Standard Oracle (non-RDS):**
```sql
-- Connect as SYSDBA
ALTER SYSTEM SET enable_goldengate_replication=TRUE SCOPE=BOTH;
```

**Amazon RDS for Oracle:**
- Modify the DB parameter group: set `enable_goldengate_replication` to `TRUE`
- Apply the parameter group changes and **reboot the RDS instance**

**Verify:**
```sql
SELECT VALUE FROM V$PARAMETER WHERE NAME = 'enable_goldengate_replication';
-- Must return TRUE
```

### Step 2: Enable ARCHIVELOG Mode

**Standard Oracle (non-RDS):**
```sql
-- Check current mode
SELECT LOG_MODE FROM V$DATABASE;

-- Enable if not already ARCHIVELOG (requires restart)
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
```

**Amazon RDS for Oracle:**
- ARCHIVELOG mode is enabled automatically when **automated backups** are turned on
- Ensure automated backups are enabled in the RDS console (backup retention period > 0)

### Step 3: Configure Supplemental Logging

Supplemental logging ensures all column values are included in the redo log for CDC.

**Minimal supplemental logging (required):**
```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

**Table-level supplemental logging (recommended — captures all columns):**
```sql
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

**Alternative — database-level (captures all columns for all tables):**
```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

**Amazon RDS for Oracle:**
```sql
-- Enable minimal supplemental logging
exec rdsadmin.rdsadmin_util.alter_supplemental_logging('ADD');

-- Enable ALL column supplemental logging (database-level)
exec rdsadmin.rdsadmin_util.alter_supplemental_logging('ADD','ALL');

-- Or table-level (same SQL as standard Oracle)
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

**Verify:**
```sql
SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE;
-- SUPPLEMENTAL_LOG_DATA_MIN should be YES

-- For table-level verification
SELECT * FROM ALL_LOG_GROUPS
WHERE LOG_GROUP_TYPE = 'ALL COLUMN LOGGING'
  AND OWNER = '<schema>'
  AND TABLE_NAME = '<table>';
```

### Step 4: Create XStream Admin User and Grant Privileges

```sql
-- Create the XStream admin user
CREATE USER xstream_admin IDENTIFIED BY <password>;
GRANT CREATE SESSION TO xstream_admin;
GRANT SET CONTAINER TO xstream_admin;

-- Grant XStream admin privileges
BEGIN
  DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
    grantee => 'xstream_admin',
    privilege_type => 'CAPTURE',
    grant_select_privileges => TRUE
  );
END;
/
```

### Step 5: Create XStream Outbound Server

The outbound server name must match the connector's `database.out.server.name` property.

```sql
-- Connect as the XStream admin user
BEGIN
  DBMS_XSTREAM_ADM.CREATE_OUTBOUND(
    server_name => 'dbz_outbound',
    table_names => '<SCHEMA>.<TABLE1>,<SCHEMA>.<TABLE2>',
    source_database => '<database_name>'
  );
END;
/
```

**Important:** The `table_names` parameter controls which tables are captured. If you need to add more tables later, you must drop and recreate the outbound server, or use `DBMS_XSTREAM_ADM.ALTER_OUTBOUND` to add tables.

### Step 6: Create Connector User and Grant Permissions

The connector user is the user specified in the connector's `database.user` property — this is the user the connector authenticates as to read XStream changes.

```sql
-- Create connector user
CREATE USER <connector_user> IDENTIFIED BY <password>;

-- Grant required permissions
GRANT CREATE SESSION TO <connector_user>;
GRANT SET CONTAINER TO <connector_user>;
GRANT SELECT ON V_$DATABASE TO <connector_user>;
GRANT SELECT ON V_$INSTANCE TO <connector_user>;
GRANT FLASHBACK ANY TABLE TO <connector_user>;
GRANT SELECT_CATALOG_ROLE TO <connector_user>;
GRANT EXECUTE_CATALOG_ROLE TO <connector_user>;
GRANT SELECT ANY TABLE TO <connector_user>;
GRANT LOCK ANY TABLE TO <connector_user>;

-- Grant XStream connect privilege to the connector user for the outbound server
BEGIN
  DBMS_XSTREAM_ADM.ALTER_OUTBOUND(
    server_name => 'dbz_outbound',
    connect_user => '<connector_user>'
  );
END;
/
```

### Step 7: Verify Complete Setup

```sql
-- 1. GoldenGate replication enabled
SELECT VALUE FROM V$PARAMETER WHERE NAME = 'enable_goldengate_replication';
-- Must return TRUE

-- 2. ARCHIVELOG mode
SELECT LOG_MODE FROM V$DATABASE;
-- Must return ARCHIVELOG

-- 3. Supplemental logging
SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE;
-- SUPPLEMENTAL_LOG_DATA_MIN should be YES

-- 4. XStream outbound server exists and is running
SELECT SERVER_NAME, CONNECT_USER, STATUS FROM ALL_XSTREAM_OUTBOUND;
-- Should show your outbound server with ATTACHED or ENABLED status

-- 5. Table-level supplemental logging (if using table-level)
SELECT * FROM ALL_LOG_GROUPS
WHERE LOG_GROUP_TYPE = 'ALL COLUMN LOGGING'
  AND OWNER = '<schema>'
  AND TABLE_NAME = '<table>';
```

### Connector Configuration Reference

After all prerequisites are met, the connector requires these Oracle-specific properties:

| Property | Description | Example |
|---|---|---|
| `connector.class` | Must be `OracleXStreamSource` | `OracleXStreamSource` |
| `database.hostname` | Oracle host | `oracle.example.com` |
| `database.port` | Oracle listener port | `1521` |
| `database.user` | Connector user (Step 6) | `cfltuser` |
| `database.password` | Connector user password | `***` |
| `database.dbname` | Oracle service name or SID | `ORCL` |
| `database.service.name` | **Required.** Oracle service name | `ORCL` |
| `database.out.server.name` | XStream outbound server name (Step 5) | `dbz_outbound` |
| `database.processor.licenses` | **Required.** Oracle processor license count | `1` |
| `table.include.list` | Tables to capture (uppercase) | `SCHEMA.TABLE` |

See `references/connector-configs.md` for the full connector configuration template.

---

## DynamoDB CDC Prerequisites

### DynamoDB Streams

Enable DynamoDB Streams on the table:

```bash
# Using AWS CLI
aws dynamodb update-table \
  --table-name <table-name> \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

**Stream View Types:**
- `NEW_IMAGE`: Only new item after modification
- `OLD_IMAGE`: Only old item before modification
- `NEW_AND_OLD_IMAGES`: Both (recommended for CDC)
- `KEYS_ONLY`: Only key attributes

**Recommendation:** Use `NEW_AND_OLD_IMAGES` for full CDC capabilities.

### IAM Permissions

Create IAM user or role with these permissions. **Critical:** The CDC phase requires write permissions for a KCL-style checkpointing table that the connector creates to track shard leases. Without these write permissions, the snapshot phase will work but CDC streaming will silently fail — no real-time changes will be captured.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DynamoDBReadAndCheckpointing",
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "dynamodb:DescribeStream",
        "dynamodb:DescribeTimeToLive",
        "dynamodb:DescribeContinuousBackups",
        "dynamodb:DescribeLimits",
        "dynamodb:GetRecords",
        "dynamodb:GetShardIterator",
        "dynamodb:ListStreams",
        "dynamodb:ListTables",
        "dynamodb:ListGlobalTables",
        "dynamodb:ListTagsOfResource",
        "dynamodb:Scan",
        "dynamodb:CreateTable",
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:DeleteTable",
        "dynamodb:TagResource"
      ],
      "Resource": "*"
    },
    {
      "Sid": "TagAndCloudWatch",
      "Effect": "Allow",
      "Action": [
        "tag:GetResources",
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
```

### Verification

```bash
aws dynamodb describe-table --table-name <table-name> | jq '.Table.StreamSpecification'
```

### Documentation

- **Confluent DynamoDB CDC Source connector:** https://docs.confluent.io/cloud/current/connectors/cc-amazon-dynamodb-cdc-source.html
- **DynamoDB Streams:** https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
- **DynamoDB Developer Guide:** https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html
