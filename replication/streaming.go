package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	"time"
)

type WALPayload struct {
	NextLSN string `json:"nextlsn"`
}

// Starts streaming changes on given replication slot
// using replication connection
// Takes in cancellable context that will stop the
// streaming process when cancelled
// Takes in onMessage handler function that will
// process WalMessages received by the streaming process
func StreamChanges(
	logger *logrus.Logger,
	ctx context.Context,
	replConn *pgx.ReplicationConn,
	slotName string,
	state *State,
	onMessage func(*pgx.WalMessage),
) error {
	// Options for wal2json (as documented here https://github.com/eulerto/wal2json#parameters)
	wal2JsonPluginOptions := []string{
		// Include "nextlsn" field in payload so we can update our state
		`"include-lsn" 'true'`,
		// Don't indent the JSON payload
		`"pretty-print" 'false'`,
		// Include timestamp and column OIDs in addition to detailed type names
		`"include-timestamp" 'true'`,
		`"include-type-oids" 'true'`,
	}

	// Start replication on replication slot
	err := replConn.StartReplication(slotName, state.CurrentLSN, -1, wal2JsonPluginOptions...)
	if err != nil {
		return fmt.Errorf("could not start replication: %w", err)
	}

	// Start sending heartbeats to the server to keep on streaming
	go func() {
		err := sendReplicationHeartbeat(logger, ctx, replConn, state)
		if err != nil {
			// TODO Shut down with error
			logger.Errorf("Could not send replication heartbeat: %s", err.Error())
		}
	}()

	logger.Tracef("Now streaming changes and waiting for WAL messages")

	// Listen for incoming WAL messages until context
	// is cancelled or replication connection dies
	for {
		// Check if context was cancelled
		if ctx.Err() == context.Canceled {
			return nil
		}

		// Check replication connection health
		if !replConn.IsAlive() {
			return fmt.Errorf("replication connection unhealthy: %w", replConn.CauseOfDeath())
		}

		// Wait for incoming replication messages, pass in cancellable context
		message, err := replConn.WaitForReplicationMessage(ctx)
		if err != nil {
			return fmt.Errorf("could not wait for replication message: %w", err)
		}

		// Handle server heartbeats, respond if asked to
		serverHeartbeat := message.ServerHeartbeat
		if serverHeartbeat != nil {
			logger.Tracef("Got server heartbeat: %s", serverHeartbeat.String())

			// Handle server heartbeat reply requests
			if serverHeartbeat.ReplyRequested == 1 {
				err = sendStandbyStatus(logger, replConn, state)
				if err != nil {
					return fmt.Errorf("could not reply heartbeat requested by server: %w", err)
				}
			}
		}

		// Handle WAL messages
		walMessage := message.WalMessage
		if walMessage == nil {
			continue
		}

		logger.Tracef("Got WAL message: %s", walMessage.String())

		// Handle onMessage hook if supplied
		if onMessage != nil {
			logger.Tracef("Starting onMessage hook")
			start := time.Now()

			// Run onMessage hook
			onMessage(walMessage)

			d := time.Since(start)
			logger.WithField("duration", d.String()).Tracef("Completed onMessage hook in %s", d.String())

			// Warn user in debug mode when handler takes more than a second to complete
			if d > time.Second*1 {
				logger.Debugf("Handler took longer than one second to complete!" + " " +
					"Please make sure that your handlers don't take up too much time, otherwise we can't process the queue in real-time.")
			}
		}

		// Unmarshal WAL message data to access "nextlsn" field of wal2json
		var payload WALPayload
		err = json.Unmarshal(walMessage.WalData, &payload)
		if err != nil {
			return fmt.Errorf("could not unmarshal wal payload: %w", err)
		}

		logger.Tracef("Will update LSN of replication slot to %q", payload.NextLSN)

		updatedLSN, err := pgx.ParseLSN(payload.NextLSN)
		if err != nil {
			return fmt.Errorf("could not parse wal payload lsn: %w", err)
		}

		state.CurrentLSN = updatedLSN

		// Acknowledge message so Postgres does not resend it eventually
		err = sendStandbyStatus(logger, replConn, state)
		if err != nil {
			return fmt.Errorf("could not refresh lsn after wal message: %w", err)
		}
	}
}
