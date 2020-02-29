package lode

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"testing"
	"time"
)

func TestCreate(t *testing.T) {
	// Create and launch lode instance
	done, cancel, err := Create(Configuration{
		LogLevel:         logrus.TraceLevel,
		ConnectionString: "postgresql://postgres:password@localhost:5432/postgres",
	})
	if err != nil {
		t.Fatalf("failed to create: %s", err.Error())
	}

	// Stop down after ten seconds
	go func() {
		<-time.After(10 * time.Second)
		cancel()
	}()

	result := <-done

	if !errors.Is(result.Error, context.Canceled) {
		t.Fatalf("received different error than expected: %s", result.Error.Error())
	}
}
