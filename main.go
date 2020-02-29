package lode

import (
	"context"
	"fmt"
	"github.com/brunoscheufler/lode/replication"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
)

type Configuration struct {
	LogLevel         logrus.Level
	ConnectionString string
}

func Create(configuration Configuration) (<-chan int, context.CancelFunc, error) {
	logger := logrus.New()

	logger.SetLevel(configuration.LogLevel)

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

	slotName, state, err := replication.Setup(logger, pgConn, replConn)
	if err != nil {
		return nil, nil, fmt.Errorf("could not setup Postgres replication: %w", err)
	}

	done := make(chan int)

	// Create root context
	rootCtx := context.Background()
	streamCtx, cancel := context.WithCancel(rootCtx)

	go func() {
		err := replication.StreamChanges(logger, streamCtx, replConn, slotName, state)
		if err != nil {
			logger.Errorf("Could not stream changes: %s", err.Error())
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

		logger.Infof("Done!")

		done <- 0
	}()

	return done, cancel, nil
}
