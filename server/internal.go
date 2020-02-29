package server

import (
	"context"
	log "github.com/sirupsen/logrus"
	"net/http"
)

func LaunchInternalServer(cancel context.CancelFunc) *http.Server {
	server := &http.Server{
		// TODO Make dynamic
		Addr: ":8080",
	}

	http.HandleFunc("/shutdown", func(writer http.ResponseWriter, request *http.Request) {
		cancel()
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("OK"))
	})

	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Errorf("Could not run server: %s", err.Error())
			cancel()
		}
	}()

	return server
}
