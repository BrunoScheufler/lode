package lode

import (
	"context"
	"fmt"
	"github.com/brunoscheufler/lode/replication"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
)

type Configuration struct {
	// Postgres connection string to use
	ConnectionString string

	// Postgres replication slot name override
	// Will default to "lode_main"
	SlotName string

	// Handle incoming WAL message
	OnMessage func(*pgx.WalMessage)

	// Pass existing logger instance
	Logger *logrus.Logger

	// Pass log level to use when logger should be created
	LogLevel logrus.Level
}

// Result type returned by channel on lode exit
// Can contain streaming error
type ExitResult struct {
	// Error returned from streaming goroutine
	Error error
}

func Create(configuration Configuration) (<-chan ExitResult, context.CancelFunc, error) {
	logger := configuration.Logger
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(configuration.LogLevel)
	}

	// Parse connection string to config
	parsedConnectConfig, err := pgx.ParseConnectionString(configuration.ConnectionString)
	if err != nil {
		return nil, nil, fmt.Errorf("could not parse connection string: %w", err)
	}

	// Create regular connection
	pgConn, err := pgx.Connect(parsedConnectConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("could not establish regular Postgres connection: %w", err)
	}

	logger.Debugf("Established regular pg connection")

	// Create replication connection
	replConn, err := pgx.ReplicationConnect(parsedConnectConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("could not establish replication connection: %w", err)
	}

	logger.Debugf("Established replication pg connection")

	logger.Infof("Connected to Postgres instance, setting up replication")

	slotName, state, err := replication.Setup(logger, pgConn, replConn, configuration.SlotName)
	if err != nil {
		return nil, nil, fmt.Errorf("could not setup Postgres replication: %w", err)
	}

	done := make(chan ExitResult)

	// Create root context
	rootCtx := context.Background()
	streamCtx, cancel := context.WithCancel(rootCtx)

	// Stream changes asynchronously until the context is cancelled or something bad happens
	go func() {
		streamErr := replication.StreamChanges(logger, streamCtx, replConn, slotName, state, configuration.OnMessage)
		if streamErr != nil {
			logger.Errorf("Could not stream changes: %s", streamErr.Error())
		}

		// Shut down both connections gracefully before exiting
		err = replConn.Close()
		if err != nil {
			logger.Errorf("Could not close replication connection: %s", err.Error())
		}

		err = pgConn.Close()
		if err != nil {
			logger.Errorf("Could not close regular pg connection: %s", err.Error())
		}

		logger.Trace("Done shutting down lode!")

		done <- ExitResult{
			Error: streamErr,
		}
	}()

	return done, cancel, nil
}
