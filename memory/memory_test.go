package store

import (
	"testing"
	"time"

	"charlesbases/store"
)

var s store.Store

func init() {
	s = NewStore()
}

func TestWrite(t *testing.T) {
	s.Write(&store.Record{
		Key:   "test write",
		Value: []byte("test write"),
		TTL:   time.Second * 5,
	})
}

func TestRead(t *testing.T) {
	s.Read("test write")
}

func TestList(t *testing.T) {
	s.List()
}
