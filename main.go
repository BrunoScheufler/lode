package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"time"
)

const ReplicationSlotPrefix string = "lode"
const StandbyStatusInterval = 10 * time.Second

func main() {
	// TODO Make dynamic
	log.SetLevel(log.TraceLevel)

	// Parse connection string to config
	// TODO Make connection string dynamic
	parsedConnectConfig, err := pgx.ParseConnectionString("postgresql://postgres:password@localhost:5432/postgres")
	if err != nil {
		log.Fatalf("Could not parse connection string: %s", err.Error())
		return
	}

	// Create regular connection
	pgConn, err := pgx.Connect(parsedConnectConfig)
	if err != nil {
		log.Fatalf("Could not establish regular Postgres connection: %s", err.Error())
		return
	}

	log.Debugf("Established regular pg connection")

	// Create replication connection
	replConn, err := pgx.ReplicationConnect(parsedConnectConfig)
	if err != nil {
		log.Fatalf("Could not establish replication connection: %s", err.Error())
		return
	}

	log.Debugf("Established replication pg connection")

	log.Infof("Connected to Postgres instance, dropping existing replication slots")

	// Drop all existing lode replication slots
	err = dropExistingReplicationSlots(pgConn, replConn)
	if err != nil {
		log.Fatalf("Could not drop existing replication slots: %s", err.Error())
		return
	}

	log.Infof("Dropped existing replication slots!")

	// Create replication slot
	slotName := fmt.Sprintf("%s_main", ReplicationSlotPrefix)
	lsn, snapshotName, err := initializeReplicationSlot(slotName, replConn)
	if err != nil {
		log.Fatalf("Could not initialize replication slot: %s", err.Error())
		return
	}

	log.WithFields(log.Fields{
		"lsn":          lsn,
		"snapshotName": snapshotName,
		"slotName":     slotName,
	}).Infof("Created wal2json replication slot")

	// Create root context
	ctx := context.Background()

	err = streamChanges(ctx, replConn, slotName, lsn)
	if err != nil {
		log.Fatalf("Could not stream changes: %s", err.Error())
		return
	}

	// Shut down both connections gracefully before exiting
	err = replConn.Close()
	if err != nil {
		log.Errorf("Could not close replication connection: %s", err.Error())
	}

	err = pgConn.Close()
	if err != nil {
		log.Error("Could not close regular pg connection: %s", err.Error())
	}

	log.Infof("Done!")
}

func dropExistingReplicationSlots(pgConn *pgx.Conn, replConn *pgx.ReplicationConn) error {
	// Fetch all replication slots that contain prefix
	rows, err := pgConn.Query("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE $1;", fmt.Sprintf("%s%%", ReplicationSlotPrefix))
	if err != nil {
		return fmt.Errorf("could not query for replication slots: %w", err)
	}

	// Iterate over returned rows
	for rows.Next() {
		// Fetch slot name out of row
		var slotName string
		err := rows.Scan(&slotName)
		if err != nil {
			return fmt.Errorf("could not scan slot name: %w", err)
		}

		// Drop relication slot
		err = replConn.DropReplicationSlot(slotName)
		if err != nil {
			return fmt.Errorf("could not delete replication slot %q: %w", slotName, err)
		}
	}

	return nil
}

func initializeReplicationSlot(slotName string, replConn *pgx.ReplicationConn) (uint64, string, error) {
	// Create wal2json replication slot and return consistent point + snapshot name
	consistentPoint, snapshotName, err := replConn.CreateReplicationSlotEx(slotName, "wal2json")
	if err != nil {
		return 0, "", fmt.Errorf("could not create")
	}

	// Parse LSN from consistent point
	lsn, err := pgx.ParseLSN(consistentPoint)

	return lsn, snapshotName, nil
}

func streamChanges(ctx context.Context, replConn *pgx.ReplicationConn, slotName string, restartLSN uint64) error {
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
		}

		walMessage := message.WalMessage
		if walMessage == nil {
			continue
		}

		log.Infof("Got change: %s", string(walMessage.WalData))
	}
}

// send Postgres standby status (heartbeat) to keep
// streaming changes using replication connection
func sendReplicationHeartbeat(ctx context.Context, replConn *pgx.ReplicationConn, lsn *uint64) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(StandbyStatusInterval):
			// send standby status with current lsn
			err := sendStandbyStatus(replConn, lsn)
			if err != nil {
				return fmt.Errorf("could not send standby status: %w", err)
			}
		}
	}
}

// sendStandbyStatus sends a StandbyStatus object with the current lsn value to the server
func sendStandbyStatus(replConn *pgx.ReplicationConn, lsn *uint64) error {
	// Create standby status form restart LSN
	standbyStatus, err := pgx.NewStandbyStatus(*lsn)
	if err != nil {
		return fmt.Errorf("could not create StandbyStatus: %w", err)
	}

	// Save standby status (send heartbeat)
	err = replConn.SendStandbyStatus(standbyStatus)
	if err != nil {
		return fmt.Errorf("could not send StandbyStatus: %w", err)
	}

	return nil
}
