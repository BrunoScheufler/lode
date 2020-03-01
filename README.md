# lode

[![godoc](https://godoc.org/github.com/brunoscheufler/lode?status.svg)](https://godoc.org/github.com/brunoscheufler/lode)

> A toolkit to build infrastructure around real-time Postgres change-streaming

## about lode

`lode` (abbreviation for `logical decoding`, based on the [Postgres feature](https://www.postgresql.org/docs/current/logicaldecoding.html) it uses under the hood),
is a long-running service which receives a continuous change stream from your Postgres instance and allows you to build custom functionality around it. This equips
Postgres with powerful real-time abilities in addition to [`LISTEN/NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html), which is often used for sending
messages across services, or automating similar workflows to what lode is built for. `lode` was heavily inspired by [pgdeltastream](https://github.com/hasura/pgdeltastream).

## background information

Postgres keeps track of all transactions in a so-called [write-ahead log](https://www.postgresql.org/docs/current/wal-intro.html), WAL in short. Used for recovery and internal
maintenance tasks, it can also function as a change feed for replication in clusters of connected database instances, or for point-in-time recovery. [Logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html)
is a method to allow subscribers to keep their own replication state of a database, starting with a snapshot of the complete data set and then applying changes to it as they come in,
which will result in a perfect copy.

`lode` will register or reuse an existing logical replication slot with [wal2json](https://github.com/eulerto/wal2json) configured as its output plugin, which allows to capture missed events,
in case of a potential downtime. After initializing, it'll listen for changes in your database, such as `INSERTs`, `UPDATEs` or whatever else might be happening. When changes
are made, lode allows you to hook into the lifecycle and perform stream-processing workloads, after which it will acknowledge the message and let Postgres know not to resend it.
Due to the nature of streams, it's recommended to spend as little time as possible on processing each message, as you would otherwise end up with a never-ending queue of unprocessed items. 

## prerequisites

You will need a Postgres instance with the following configuration at your disposal:

- [ ] [`wal2json`](https://github.com/eulerto/wal2json) available 
- [ ] `wal_level` set to `logical`
- [ ] `max_replication_slots` set to more than one (or greater than equals the number of replication slots used)

To get a compatible database instance up and running quickly, you can start a Docker container running `debezium/postgres:12-alpine`, started with 

```bash
docker run -e POSTGRES_PASSWORD=<password> -it -p 5432:5432 debezium/postgres:12-alpine
```

## getting started

It takes less than five minutes to set up a streaming server to listen for Postgres changes and process
each item. Let's assume you're running Postgres on your machine, as explained above.
`lode` is built on Go modules, so you can use it by installing `github.com/brunoscheufler/lode`.

### configuration

Visit the [godoc](https://godoc.org/github.com/brunoscheufler/lode) to learn about possible configuration methods.

### basic WAL streaming

This example allows you to set up a `lode` instance to log all events occurring in your database.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/brunoscheufler/lode"
	"github.com/brunoscheufler/lode/parser"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
)

func main (){
	done, _, err := lode.Create(lode.Configuration{
		// Connect to local Postgres container
		ConnectionString: "postgresql://postgres:<password set earlier>@localhost:5432/postgres",

		// Handle incoming WAL messages
		OnMessage: func(message *pgx.WalMessage) error {
			// Decode wal2json payload
			payload, err := parser.ParseWal2JsonPayload(message.WalData)
			if err != nil {
				return fmt.Errorf("could not parse wal2json payload: %w", err)
			}

			// Process each change
			for _, change := range payload.Change {
				logrus.Infof("Got %s change in %q.%q", change.Kind, change.Schema, change.Table)

				// TODO Handle change
			}

			return nil
		},
	})

	// Handle startup errors
	if err != nil {
		panic(err)
	}

	// Wait until lode stops streaming (error cases, manual shutdown)
	result := <- done

	// Check if stream failed (exclude manual shutdowns which return context cancellation error)
	if result.Error != nil && errors.Is(result.Error, context.Canceled) {
		panic(err)
	}
}
```

### adding interactive cancellation

You can alter the previous example slightly to use the `cancel` function exposed by `Create`
to get the ability to shut down the streaming process whenever you want. This is especially useful
if you're planning to run `lode` asynchronously.

```go
func main (){
	done, cancel, err := lode.Create(lode.Configuration{
        // same as in the example above
	})

    // Handle startup errors
	if err != nil {
		panic(err)
	}
	
    // Shut down server after ten seconds
	go func() {
        <-time.After(10 * time.Second)
		
        cancel()
	}()

	// Wait until lode stops streaming (error cases, manual shutdown)
	result := <- done

	// Check if stream failed (exclude manual shutdowns which return context cancellation error)
	if result.Error != nil && errors.Is(result.Error, context.Canceled) {
		panic(err)
	}
}
```

### reading the payload

Since we use [wal2json](https://github.com/eulerto/wal2json) as the output plugin for lode, all messages we receive are in the wal2json format (version 1).

#### notes on replica identity

By default, all non-creating (so `UPDATE`, `DELETE`) operations only show old keys and changes to those columns. If you want to receive _all_ columns to generate
a diff of previous values to current values (to see what changed in an operation), you need to alter the [replica identity](https://www.postgresql.org/docs/current/logical-replication-publication.html)
of each table you want to diff. An [issue](https://github.com/eulerto/wal2json/issues/7) in the wal2json repository covers the
expected and default behaviour. To switch a table's replica identity, run

```sql
ALTER TABLE "<your table>" REPLICA IDENTITY FULL;
``` 

To reset the replica identity, simply run the same query above and set it to `DEFAULT` instead of `FULL`.
