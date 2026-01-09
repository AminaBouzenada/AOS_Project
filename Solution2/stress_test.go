package main

import (
	"sync"
	"testing"
)

// stress test is a concurrency test with high number of clients and increments.
func TestStress(t *testing.T) {
	db := NewDatabase()

	// Initialize counter to 0
	initTx := db.BeginTransaction()
	db.Write(initTx, "counter", 0)
	db.Commit(initTx)

	numClients := 200
	incrementsPerClient := 200

	var wg sync.WaitGroup

	// Each client increments the counter
	for i := 0; i < numClients; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < incrementsPerClient; j++ {

				tx := db.BeginTransaction()

				db.Update(tx, "counter", 1) // Increment by 1

				db.Commit(tx)
			}
		}()
	}

	wg.Wait()

	finalValue, _ := db.Read(db.BeginTransaction(), "counter")
	expectedValue := numClients * incrementsPerClient

	if finalValue != expectedValue {
		t.Fatalf("Stress test failure : expected %d, got %d", expectedValue, finalValue)
	}

}
