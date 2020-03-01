package parser

import (
	"encoding/json"
	"fmt"
)

type Wal2JsonChange struct {
	// Operation kind (e.g. insert, update, delete)
	Kind string `json:"kind"`

	// Schema & Table names
	Schema string `json:"schema"`
	Table  string `json:"table"`

	// Column names
	ColumnNames []string `json:"columnnames"`

	// Human-readable column types
	ColumnTypes []string `json:"columntypes"`

	// Column types in Postgres OID format
	ColumnTypeOIDs []float64 `json:"columntypeoids"`

	// Current column values (after the operation)
	ColumnValues []interface{} `json:"columnvalues"`

	// Present on UPDATE operations, contains affected
	// keys and, if table replica identity is set to FULL,
	// previous values
	OldKeys struct {
		KeyNames []string `json:"keynames"`

		KeyTypes    []string  `json:"keytypes"`
		KeyTypeOIDs []float64 `json:"keytypeoids"`

		KeyValues []interface{} `json:"keyvalues"`
	} `json:"oldkeys"`
}

type Wal2JsonMessage struct {
	NextLSN   string           `json:"nextlsn"`
	Timestamp string           `json:"timestamp"`
	Change    []Wal2JsonChange `json:"change"`
}

// Parses wal2json payload (as included in pgx.WalMessage as WalData)
// to Wal2JsonMessage struct
func ParseWal2JsonPayload(payload []byte) (*Wal2JsonMessage, error) {
	var message Wal2JsonMessage
	err := json.Unmarshal(payload, &message)
	if err != nil {
		return nil, fmt.Errorf("could not parse wal2json payload to wal2json message: %w", err)
	}

	return &message, nil
}
