# lode

> A service to build tools based on Postgres change-streaming

## about lode

Lode (abbreviation for `logical decoding`, based on the [Postgres feature](https://www.postgresql.org/docs/current/logicaldecoding.html) it uses under the hood),
is a long-running service which receives a continuous change stream from your Postgres instance and allows you to build custom functionality around it. This equips
Postgres with powerful real-time abilities in addition to [`LISTEN/NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html), which is often used for sending
messages across services, or automating similar workflows to what lode is built for.

## background information

Postgres keeps track of all transactions in a so-called [write-ahead log](https://www.postgresql.org/docs/current/wal-intro.html), WAL in short. Used for recovery and internal
maintenance tasks, it can also function as a change feed for replication in clusters of connected database instances, or for point-in-time recovery. Logical replication is a
method to allow subscribers to keep their own replication state of a database, starting with a snapshot of the complete data set and then applying changes to it as they come in,
which will result in a perfect copy.

Lode will register or reuse an existing logical replication slot with [wal2json](https://github.com/eulerto/wal2json) configured as its plugin, which allows to capture _missed_ events,
in case of a potential downtime. After initializing, it'll listen for changes in your database, such as `INSERTs`, `UPDATEs` or whatever else might be happening. When changes
are made, lode allows you to hook into the lifecycle and perform stream-processing workloads, after which it will acknowledge the message and let Postgres know not to resend it.
Due to the nature of streams, it's recommended to spend as little time as possible on processing each message, as you would otherwise end up with a never-ending queue of unprocessed items. 

## prerequisites

You need a Postgres instance with

- [ ] [`wal2json`](https://github.com/eulerto/wal2json) available 
- [ ] `wal_level` set to `logical`
- [ ] `max_replication_slots` set to more than one (or greater than equals the number of replication slots used)

To get a database instance up and running quickly, you can start a Docker container running `debezium/postgres:12-alpine`, started with 

```bash
docker run -e POSTGRES_PASSWORD=<password for "postgres" user> -it -p 5432:5432 debezium/postgres:12-alpine
```

## getting started

TBA

```go
```

## debugging

To check the current WAL state it can be helpful to see differences in the database state and your replication slot,
especially when restarting and trying to resend changes that happened while lode wasn't listening. An example query
to calculate differences between the current WAL LSN and the slot's last checkpoint is added below.

```sql
WITH "slot" AS (
  SELECT * FROM pg_replication_slots WHERE slot_name = '<lode slot name>'
) SELECT
  pg_wal_lsn_diff(slot.restart_lsn, pg_current_wal_lsn()) AS slot_current_diff,
  slot.restart_lsn AS restart_lsn,
  slot.confirmed_flush_lsn AS "confirmed_flush_lsn",
  pg_current_wal_lsn() AS "current_lsn"
FROM "slot";
```
