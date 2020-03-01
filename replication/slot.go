package replication

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	"time"
)

const DefaultSlotName = "lode_main"
const StandbyStatusInterval = 10 * time.Second

type State struct {
	CurrentLSN uint64
}

func Setup(logger *logrus.Logger, pgConn *pgx.Conn, replConn *pgx.ReplicationConn, slotNameOverride string) (string, *State, error) {
	slotName := DefaultSlotName
	if slotNameOverride != "" {
		slotName = slotNameOverride
	}

	initialLSN, err := fetchReplicationSlot(pgConn, slotName)
	if err != nil {
		return "", nil, fmt.Errorf("could not fetch replication slot: %w", err)
	}

	if initialLSN == 0 {
		// Create replication slot
		initialLSN, err = createReplicationSlot(slotName, replConn)
		if err != nil {
			return "", nil, fmt.Errorf("could not initialize replication slot: %w", err)
		}

		logger.WithFields(logrus.Fields{
			"lsn":      initialLSN,
			"slotName": slotName,
		}).Infof("Created wal2json replication slot")
	}

	logger.WithFields(logrus.Fields{
		"lsn":      pgx.FormatLSN(initialLSN),
		"slotName": slotName,
	}).Infof("Set up replication slot")

	return slotName, &State{
		CurrentLSN: initialLSN,
	}, nil
}

// Create new replication slot using wal2json output plugin and preset slotName,
// returning the initial LSN (consistent point) to start streaming with
func createReplicationSlot(slotName string, replConn *pgx.ReplicationConn) (uint64, error) {
	// Create wal2json replication slot and return consistent point + snapshot name
	consistentPoint, _, err := replConn.CreateReplicationSlotEx(slotName, "wal2json")
	if err != nil {
		return 0, fmt.Errorf("could not create")
	}

	// Parse LSN from consistent point
	lsn, err := pgx.ParseLSN(consistentPoint)

	return lsn, nil
}

// Retrieve replication slot restart_lsn by name, returns 0 if no results are returned
func fetchReplicationSlot(pgConn *pgx.Conn, slotName string) (uint64, error) {
	// Fetch restart_lsn from replication slot
	rows, err := pgConn.Query("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = $1;", slotName)
	if err != nil {
		return 0, fmt.Errorf("could not query for replication slots: %w", err)
	}

	// Iterate over returned rows
	for rows.Next() {
		// Fetch restart LSN out of row
		var restartLsn string
		err := rows.Scan(&restartLsn)
		if err != nil {
			return 0, fmt.Errorf("could not scan slot name: %w", err)
		}

		startLsn, err := pgx.ParseLSN(restartLsn)
		if err != nil {
			return 0, fmt.Errorf("could not parse restart LSN: %w", err)
		}

		return startLsn, nil
	}

	return 0, nil
}

// send Postgres standby status (heartbeat) to keep
// streaming changes using replication connection
func sendReplicationHeartbeat(logger *logrus.Logger, ctx context.Context, replConn *pgx.ReplicationConn, state *State) error {
	for {
		select {
		// when context is closed, return immediately
		case <-ctx.Done():
			return ctx.Err()
		// otherwise send heartbeat in interval
		case <-time.After(StandbyStatusInterval):
			// send standby status with current lsn
			err := sendStandbyStatus(logger, replConn, state)
			if err != nil {
				return fmt.Errorf("could not send standby status: %w", err)
			}
		}
	}
}

// sendStandbyStatus sends a StandbyStatus object with the current lsn value to the server
func sendStandbyStatus(logger *logrus.Logger, replConn *pgx.ReplicationConn, state *State) error {
	currentLsn := state.CurrentLSN

	// Create standby status form restart LSN
	standbyStatus, err := pgx.NewStandbyStatus(currentLsn)
	if err != nil {
		return fmt.Errorf("could not create StandbyStatus: %w", err)
	}

	logger.Tracef("Sending standby status with LSN %q", pgx.FormatLSN(currentLsn))

	// Save standby status (send heartbeat)
	err = replConn.SendStandbyStatus(standbyStatus)
	if err != nil {
		return fmt.Errorf("could not send StandbyStatus: %w", err)
	}

	return nil
}
