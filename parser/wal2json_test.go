package parser

import (
	"github.com/bradleyjkemp/cupaloy"
	"testing"
)

func TestParseWalMessage(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name:    "insert message with default replica identity",
			payload: []byte(`{"nextlsn":"0/177AA68","timestamp":"2020-03-01 12:02:47.530709+00","change":[{"kind":"insert","schema":"public","table":"user","columnnames":["id","name","email","bio"],"columntypes":["integer","character varying(32)","character varying(32)","text"],"columntypeoids":[23,1043,1043,25],"columnvalues":[2,"bruno","bruno@brunoscheufler.com","hello world"]}]}`),
		},
		{
			name:    "insert message with full replica identity",
			payload: []byte(`{"nextlsn":"0/177B6A0","timestamp":"2020-03-01 12:04:08.319577+00","change":[{"kind":"insert","schema":"public","table":"user","columnnames":["id","name","email","bio"],"columntypes":["integer","character varying(32)","character varying(32)","text"],"columntypeoids":[23,1043,1043,25],"columnvalues":[3,"bruno","bruno@brunoscheufler.com","this is awesome"]}]}`),
		},
		{
			name:    "update message with full replica identity",
			payload: []byte(`{"nextlsn":"0/1779538","timestamp":"2020-03-01 11:51:12.67553+00","change":[{"kind":"update","schema":"public","table":"user","columnnames":["id","name","email","bio"],"columntypes":["integer","character varying(32)","character varying(32)","text"],"columntypeoids":[23,1043,1043,25],"columnvalues":[1,"bruno","bruno@brunoscheufler.com","2"],"oldkeys":{"keynames":["id","name","email","bio"],"keytypes":["integer","character varying(32)","character varying(32)","text"],"keytypeoids":[23,1043,1043,25],"keyvalues":[1,"bruno","bruno@brunoscheufler.com","1"]}}]}`),
		},
		{
			name:    "update message with default replica identity",
			payload: []byte(`{"nextlsn":"0/177A3D8","timestamp":"2020-03-01 11:55:02.139179+00","change":[{"kind":"update","schema":"public","table":"user","columnnames":["id","name","email","bio"],"columntypes":["integer","character varying(32)","character varying(32)","text"],"columntypeoids":[23,1043,1043,25],"columnvalues":[1,"bruno","bruno@brunoscheufler.com","3"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keytypeoids":[23],"keyvalues":[1]}}]}`),
		},
		{
			name:    "delete message with default replica identity",
			payload: []byte(`{"nextlsn":"0/177A8A8","timestamp":"2020-03-01 12:02:12.577505+00","change":[{"kind":"delete","schema":"public","table":"user","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keytypeoids":[23],"keyvalues":[1]}}]}`),
		},
		{
			name:    "delete message with full replica identity",
			payload: []byte(`{"nextlsn":"0/177B588","timestamp":"2020-03-01 12:03:30.781033+00","change":[{"kind":"delete","schema":"public","table":"user","oldkeys":{"keynames":["id","name","email","bio"],"keytypes":["integer","character varying(32)","character varying(32)","text"],"keytypeoids":[23,1043,1043,25],"keyvalues":[2,"bruno","bruno@brunoscheufler.com","hello world"]}}]}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseWal2JsonPayload(tt.payload)
			if err != nil {
				t.Fatalf("Could not parse wal2json payload: %s", err.Error())
			}

			cupaloy.New(cupaloy.SnapshotFileExtension(".snap")).SnapshotT(t, got)
		})
	}
}
