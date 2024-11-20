package main

import (
	"context"
	"testing"
)

func TestConnect(t *testing.T) {
	conn, err := connect(context.TODO(), "ch://localhost:9000/default")
	if err != nil {
		t.Errorf("error: %s", err)
		t.FailNow()
	}

	defer conn.Close()
	rs, err := conn.Query(context.TODO(), "SELECT 1")
	if err != nil {
		t.Errorf("error: %s", err)
		t.FailNow()
	}

	defer rs.Close()
	if !rs.Next() {
		t.Errorf("no rows")
		t.FailNow()
	}
}
