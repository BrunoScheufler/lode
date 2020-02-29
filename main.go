package main

import (
	"context"
	"github.com/brunoscheufler/lode/replication"
	"github.com/brunoscheufler/lode/server"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"net/http"
)

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

	lsn, slotName, err := replication.Setup(pgConn, replConn)
	if err != nil {
		log.Fatalf("Could not setup Postgres replication: %s", err.Error())
	}

	// Create root context
	rootCtx := context.Background()
	streamCtx, cancel := context.WithCancel(rootCtx)

	internalServer := server.LaunchInternalServer(cancel)

	err = replication.StreamChanges(streamCtx, replConn, slotName, lsn)
	if err != nil {
		log.Errorf("Could not stream changes: %s", err.Error())
	}

	err = internalServer.Shutdown(rootCtx)
	if err != nil && err != http.ErrServerClosed {
		log.Errorf("Could not shutdown server: %s", err.Error())
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
