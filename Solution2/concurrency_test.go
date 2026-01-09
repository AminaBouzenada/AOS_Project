package main

import (
	"sync"
	"testing"
)

// TestConcurrency tests that there is no lost of updates.
func TestConcurrency(t *testing.T) {
	db := NewDatabase()

	// Initialize counter to 0
	initTx := db.BeginTransaction()
	db.Write(initTx, "counter", 0)
	db.Commit(initTx)

	numClients := 10
	incrementsPerClient := 100

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
		t.Fatalf("Lost updates: expected %d, got %d", expectedValue, finalValue)
	}

}

func BenchmarkConcurent(b *testing.B) {
	db := NewDatabase()

	// Initialize counter to 0
	initTx := db.BeginTransaction()
	db.Write(initTx, "counter", 0)
	db.Commit(initTx)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx := db.BeginTransaction()
			db.Update(tx, "counter", 1)
			db.Commit(tx)
		}
	})

}
