package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

type WALPayload struct {
	NextLSN string `json:"nextlsn"`
}

func StreamChanges(ctx context.Context, replConn *pgx.ReplicationConn, slotName string, state *State) error {
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
	err := replConn.StartReplication(slotName, state.InitialRestartLSN, -1, wal2JsonPluginOptions...)
	if err != nil {
		return fmt.Errorf("could not start replication: %w", err)
	}

	go func() {
		err := sendReplicationHeartbeat(ctx, replConn, state)
		if err != nil {
			log.Fatalf("Could not send replication heartbeat: %s", err.Error())
		}
	}()

	for {
		if ctx.Err() == context.Canceled {
			return nil
		}

		if !replConn.IsAlive() {
			return fmt.Errorf("replication connection unhealthy: %w", replConn.CauseOfDeath())
		}

		message, err := replConn.WaitForReplicationMessage(ctx)
		if err != nil {
			return fmt.Errorf("could not wait for replication message: %w", err)
		}

		serverHeartbeat := message.ServerHeartbeat
		if serverHeartbeat != nil {
			log.Tracef("Got server heartbeat: %s", serverHeartbeat.String())

			// Handle server heartbeat reply requests
			if serverHeartbeat.ReplyRequested == 1 {
				err = sendStandbyStatus(replConn, state)
				if err != nil {
					return fmt.Errorf("could not reply heartbeat requested by server: %w", err)
				}
			}
		}

		walMessage := message.WalMessage
		if walMessage == nil {
			continue
		}

		log.Infof("Got WAL message: %s", walMessage.String())

		var payload WALPayload
		err = json.Unmarshal(walMessage.WalData, &payload)
		if err != nil {
			return fmt.Errorf("could not unmarshal wal payload: %w", err)
		}

		log.Tracef("Will update LSN of replication slot to %q", payload.NextLSN)

		updatedLSN, err := pgx.ParseLSN(payload.NextLSN)
		if err != nil {
			return fmt.Errorf("could not parse wal payload lsn: %w", err)
		}

		state.CurrentLSN = updatedLSN

		err = sendStandbyStatus(replConn, state)
		if err != nil {
			return fmt.Errorf("could not refresh lsn after wal message: %w", err)
		}
	}
}
