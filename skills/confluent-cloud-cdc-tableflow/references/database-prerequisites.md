# Database Prerequisites for CDC

Each database requires specific configuration to support Change Data Capture. This guide covers the prerequisites for each supported database type.

## PostgreSQL CDC Prerequisites

### Required PostgreSQL Configuration

**WAL Level:**
```sql
-- Check current WAL level
SHOW wal_level;

-- Must be 'logical'
-- Set in postgresql.conf:
wal_level = logical
```

**Replication Slots:**
```sql
-- Check max_replication_slots
SHOW max_replication_slots;

-- Should be at least 1 per connector (recommend 4+)
-- Set in postgresql.conf:
max_replication_slots = 4
```

**WAL Senders:**
```sql
-- Check max_wal_senders
SHOW max_wal_senders;

-- Should be at least 1 per connector (recommend 4+)
-- Set in postgresql.conf:
max_wal_senders = 4
```

**Restart Required:**
After changing these settings, PostgreSQL must be restarted.

### Required Permissions

The connector user needs these permissions:

```sql
-- Grant replication privilege
ALTER USER <connector_user> WITH REPLICATION;

-- Grant permissions on database
GRANT CONNECT ON DATABASE <database> TO <connector_user>;

-- Grant schema permissions
GRANT USAGE ON SCHEMA <schema> TO <connector_user>;

-- Grant table permissions
GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO <connector_user>;

-- Grant permissions for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA <schema>
  GRANT SELECT ON TABLES TO <connector_user>;
```

### Publication Setup (PostgreSQL 10+)

```sql
-- Create publication for tables to capture
CREATE PUBLICATION dbz_publication FOR TABLE table1, table2, table3;

-- Or for all tables in a schema
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
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

### Verification

```sql
-- Verify WAL level
SELECT name, setting FROM pg_settings WHERE name = 'wal_level';

-- Check replication slots
SELECT * FROM pg_replication_slots;

-- Test publication
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication';
```

**References:**
- Debezium PostgreSQL: https://debezium.io/documentation/reference/2.4/connectors/postgresql.html#postgresql-in-the-cloud
- Confluent PostgreSQL CDC V2: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html

---

## MySQL CDC Prerequisites

### Required MySQL Configuration

**Binary Logging:**
```sql
-- Check if binary logging is enabled
SHOW VARIABLES LIKE 'log_bin';

-- Check binlog format (must be ROW)
SHOW VARIABLES LIKE 'binlog_format';

-- Set in my.cnf or my.ini:
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
```

**GTID Mode (Recommended):**
```sql
-- Check GTID status
SHOW VARIABLES LIKE 'gtid_mode';
SHOW VARIABLES LIKE 'enforce_gtid_consistency';

-- Enable in my.cnf:
gtid_mode = ON
enforce_gtid_consistency = ON
```

**Binary Log Retention:**
```sql
-- Check retention (in seconds)
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';

-- Set retention to at least 7 days (604800 seconds)
-- In my.cnf:
binlog_expire_logs_seconds = 604800
```

**Restart Required:**
After changing these settings, MySQL must be restarted.

### Required Permissions

```sql
-- Create dedicated CDC user
CREATE USER '<connector_user>'@'%' IDENTIFIED BY '<password>';

-- Grant replication privileges
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO '<connector_user>'@'%';

-- Grant permissions on specific database/tables
GRANT SELECT ON <database>.* TO '<connector_user>'@'%';

FLUSH PRIVILEGES;
```

### Cloud-Specific Notes

**AWS RDS/Aurora:**
- Binary logging is enabled by default
- Set `binlog_format = ROW` in parameter group
- Enable automated backups (required for binlog)
- User needs `mysql.rds_set_configuration` for binlog retention

**Google Cloud SQL:**
- Enable binary logging in flags
- Set `binlog_format = ROW`
- Set retention period in days

**Azure Database for MySQL:**
- Enable binary logging in server parameters
- Set `binlog_format = ROW`
- Configure binlog retention

### Verification

```sql
-- Verify binlog is enabled and ROW format
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';

-- Check GTID mode
SHOW VARIABLES LIKE 'gtid_mode';

-- List binary logs
SHOW BINARY LOGS;

-- Verify user permissions
SHOW GRANTS FOR '<connector_user>'@'%';
```

**References:**
- Debezium MySQL: https://debezium.io/documentation/reference/2.4/connectors/mysql.html
- Confluent MySQL CDC V2: https://docs.confluent.io/cloud/current/connectors/cc-mysql-cdc-source-v2-debezium/cc-mysql-cdc-source-v2-debezium.html

---

## SQL Server CDC Prerequisites

### Enable CDC on Database

```sql
-- Check if CDC is enabled on database
SELECT is_cdc_enabled FROM sys.databases
WHERE name = '<database_name>';

-- Enable CDC on database
USE <database_name>;
EXEC sys.sp_cdc_enable_db;
```

### Enable CDC on Tables

```sql
-- Enable CDC for specific table
USE <database_name>;
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name   = N'<table_name>',
  @role_name     = NULL,
  @supports_net_changes = 1;

-- Verify CDC is enabled for table
SELECT name, is_tracked_by_cdc
FROM sys.tables
WHERE name = '<table_name>';
```

### SQL Server Agent

**Critical:** SQL Server Agent must be running for CDC to work.

```sql
-- Check SQL Server Agent status (in Management Studio or via xp_servicecontrol)
EXEC xp_servicecontrol 'QueryState', 'SQLServerAGENT';
```

### Required Permissions

```sql
-- Create login and user
CREATE LOGIN <connector_user> WITH PASSWORD = '<password>';
USE <database_name>;
CREATE USER <connector_user> FOR LOGIN <connector_user>;

-- Grant permissions
GRANT SELECT ON SCHEMA :: dbo TO <connector_user>;
GRANT VIEW DATABASE STATE TO <connector_user>;
EXEC sp_addrolemember N'db_datareader', N'<connector_user>';

-- Grant access to CDC tables
GRANT SELECT ON SCHEMA :: cdc TO <connector_user>;
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
-- Check database CDC status
SELECT name, is_cdc_enabled FROM sys.databases;

-- List CDC-enabled tables
SELECT SCHEMA_NAME(schema_id) + '.' + name AS table_name, is_tracked_by_cdc
FROM sys.tables
WHERE is_tracked_by_cdc = 1;

-- View CDC capture instances
SELECT * FROM cdc.change_tables;

-- Check CDC jobs are running
EXEC sys.sp_cdc_help_jobs;
```

**References:**
- Debezium SQL Server: https://debezium.io/documentation/reference/2.4/connectors/sqlserver.html
- Confluent SQL Server CDC V2: https://docs.confluent.io/cloud/current/connectors/cc-microsoft-sql-server-cdc-source-v2-debezium/cc-microsoft-sql-server-cdc-source-v2-debezium.html

---

## Oracle CDC Prerequisites

### Archive Log Mode

Oracle must be in ARCHIVELOG mode:

```sql
-- Check archive log mode
SELECT log_mode FROM v$database;

-- Enable archive log mode (requires restart)
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
```

### Supplemental Logging

```sql
-- Enable minimal supplemental logging at database level
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Enable identification key logging
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;

-- For tables without primary keys, enable all columns
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Verify supplemental logging
SELECT supplemental_log_data_min, supplemental_log_data_pk
FROM v$database;
```

### Table-Level Supplemental Logging

```sql
-- For each table to capture
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### XStream Configuration

For Oracle XStream CDC connector:

```sql
-- Create XStream admin user
CREATE USER xstream_admin IDENTIFIED BY <password>;
GRANT CREATE SESSION TO xstream_admin;

-- Grant XStream privileges
BEGIN
  DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
    grantee => 'xstream_admin',
    privilege_type => 'CAPTURE',
    grant_select_privileges => TRUE
  );
END;
/

-- Create outbound server
BEGIN
  DBMS_XSTREAM_ADM.CREATE_OUTBOUND(
    server_name => 'dbz_outbound',
    table_names => '<schema>.<table>',
    source_database => '<database>'
  );
END;
/
```

### Required Permissions

```sql
-- Create connector user
CREATE USER <connector_user> IDENTIFIED BY <password>;

-- Grant session and basic permissions
GRANT CREATE SESSION TO <connector_user>;
GRANT SET CONTAINER TO <connector_user>;
GRANT SELECT ON V_$DATABASE TO <connector_user>;
GRANT FLASHBACK ANY TABLE TO <connector_user>;

-- Grant table permissions
GRANT SELECT ON <schema>.<table> TO <connector_user>;
```

### Verification

```sql
-- Check archive log mode
SELECT log_mode FROM v$database;

-- Verify supplemental logging
SELECT supplemental_log_data_min, supplemental_log_data_pk, supplemental_log_data_all
FROM v$database;

-- Check XStream outbound server
SELECT server_name, capture_name, connect_user, status
FROM DBA_XSTREAM_OUTBOUND;
```

**References:**
- Debezium Oracle: https://debezium.io/documentation/reference/2.4/connectors/oracle.html
- Confluent Oracle XStream CDC: https://docs.confluent.io/cloud/current/connectors/cc-oracle-xstream-source/cc-oracle-xstream-source.html

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

Create IAM user or role with these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "dynamodb:DescribeStream",
        "dynamodb:GetRecords",
        "dynamodb:GetShardIterator",
        "dynamodb:ListStreams",
        "dynamodb:ListTables"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/<table-name>",
        "arn:aws:dynamodb:*:*:table/<table-name>/stream/*"
      ]
    }
  ]
}
```

### Verification

```bash
# Check if streams are enabled
aws dynamodb describe-table --table-name <table-name> \
  | jq '.Table.StreamSpecification'

# List streams for table
aws dynamodb list-streams --table-name <table-name>
```

**References:**
- Debezium DynamoDB: https://debezium.io/documentation/reference/2.4/connectors/dynamodb.html
- Confluent DynamoDB CDC: https://docs.confluent.io/cloud/current/connectors/cc-amazon-dynamodb-source.html
- AWS DynamoDB Streams: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html

---

## General Checklist

Before setting up any CDC connector, verify:

- [ ] Database is configured for CDC (WAL, binlog, archive log, streams)
- [ ] Database user has appropriate permissions
- [ ] Network connectivity from Confluent Cloud to database
- [ ] Firewall rules allow connection
- [ ] Schema Registry is enabled in Confluent Cloud
- [ ] Sufficient database resources for CDC overhead
- [ ] Monitoring in place for replication lag
