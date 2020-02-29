# lode

## prerequisites

You need a Postgres instance with [`wal2json`](https://github.com/eulerto/wal2json) available and `wal_level` set to `logical`.

A sample image could be `debezium/postgres:12-alpine`, started with 

```bash
docker run -it -p 5432:5432 debezium/postgres:12-alpine
```
