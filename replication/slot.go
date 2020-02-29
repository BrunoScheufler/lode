package replication

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"time"
)

const SlotPrefix string = "lode"
const StandbyStatusInterval = 10 * time.Second

type State struct {
	CurrentLSN        uint64
	InitialRestartLSN uint64
}

func Setup(pgConn *pgx.Conn, replConn *pgx.ReplicationConn) (string, *State, error) {
	// Drop all existing lode replication slots
	err := dropExistingReplicationSlots(pgConn, replConn)
	if err != nil {
		return "", nil, fmt.Errorf("could not drop existing replication slots: %w", err)
	}

	log.Infof("Dropped existing replication slots!")

	// Create replication slot
	slotName := fmt.Sprintf("%s_main", SlotPrefix)
	lsn, snapshotName, err := initializeReplicationSlot(slotName, replConn)
	if err != nil {
		return "", nil, fmt.Errorf("could not initialize replication slot: %w", err)
	}

	log.WithFields(log.Fields{
		"lsn":          lsn,
		"snapshotName": snapshotName,
		"slotName":     slotName,
	}).Infof("Created wal2json replication slot")

	return slotName, &State{
		CurrentLSN:        lsn,
		InitialRestartLSN: lsn,
	}, nil
}

func dropExistingReplicationSlots(pgConn *pgx.Conn, replConn *pgx.ReplicationConn) error {
	// Fetch all replication slots that contain prefix
	rows, err := pgConn.Query("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE $1;", fmt.Sprintf("%s%%", SlotPrefix))
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

// send Postgres standby status (heartbeat) to keep
// streaming changes using replication connection
func sendReplicationHeartbeat(ctx context.Context, replConn *pgx.ReplicationConn, state *State) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(StandbyStatusInterval):
			// send standby status with current lsn
			err := sendStandbyStatus(replConn, state)
			if err != nil {
				return fmt.Errorf("could not send standby status: %w", err)
			}
		}
	}
}

// sendStandbyStatus sends a StandbyStatus object with the current lsn value to the server
func sendStandbyStatus(replConn *pgx.ReplicationConn, state *State) error {
	currentLsn := state.CurrentLSN

	// Create standby status form restart LSN
	standbyStatus, err := pgx.NewStandbyStatus(currentLsn)
	if err != nil {
		return fmt.Errorf("could not create StandbyStatus: %w", err)
	}

	log.Tracef("Sending standby status with LSN %q", pgx.FormatLSN(currentLsn))

	// Save standby status (send heartbeat)
	err = replConn.SendStandbyStatus(standbyStatus)
	if err != nil {
		return fmt.Errorf("could not send StandbyStatus: %w", err)
	}

	return nil
}
