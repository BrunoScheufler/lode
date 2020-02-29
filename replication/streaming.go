package replication

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

func StreamChanges(ctx context.Context, replConn *pgx.ReplicationConn, slotName string, restartLSN uint64) error {
	// Options for wal2json (as documented here https://github.com/eulerto/wal2json#parameters)
	wal2JsonPluginOptions := []string{`"include-lsn" 'true'`, `"pretty-print" 'false'`}

	// Start replication on replication slot
	err := replConn.StartReplication(slotName, restartLSN, -1, wal2JsonPluginOptions...)
	if err != nil {
		return fmt.Errorf("could not start replication: %w", err)
	}

	currentLSN := &restartLSN

	go func() {
		err := sendReplicationHeartbeat(ctx, replConn, currentLSN)
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
				err = sendStandbyStatus(replConn, currentLSN)
				if err != nil {
					return fmt.Errorf("could not reply heartbeat requested by server: %w", err)
				}
			}
		}

		walMessage := message.WalMessage
		if walMessage == nil {
			continue
		}

		log.Infof("Got change: %s", string(walMessage.WalData))
	}
}
