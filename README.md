# lode

## prerequisites

You need a Postgres instance with

- [ ] [`wal2json`](https://github.com/eulerto/wal2json) available 
- [ ] `wal_level` set to `logical`
- [ ] `max_replication_slots` set to more than one (or greater than equals the number of replication slots used)

To get a database instance up and running quickly, you can start a Docker container running `debezium/postgres:12-alpine`, started with 

```bash
docker run -e POSTGRES_PASSWORD=<password for "postgres" user> -it -p 5432:5432 debezium/postgres:12-alpine
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
  slot.confirmed_flush_lsn AS "flush_older_than",
  pg_current_wal_lsn() AS "current_lsn"
FROM "slot";
```
