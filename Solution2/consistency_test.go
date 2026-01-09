package main

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestConsistencyByBankTransfer tests  Invariant preservation.
func TestConsistencyByBankTransfer(t *testing.T) {
	db := NewDatabase()

	initTx := db.BeginTransaction()
	db.Write(initTx, "account_A", 1000)
	db.Write(initTx, "account_B", 1000)
	db.Commit(initTx)

	numClients := 5
	transfersPerClient := 50

	var wg sync.WaitGroup

	// Each client will transfer money between accounts
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		clientID := i

		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)))

			for j := 0; j < transfersPerClient; j++ {

				amount := rng.Intn(50) + 1 // Transfer 1-50

				// Transfer from A to B
				tx := db.BeginTransaction()

				//lock the bloc of operations (read account_A, read account_B, write account_A, write account_B)
				db.monitor.acquireBlocLock()

				// Read from account A
				balanceA, _ := db.Read(tx, "account_A")

				// Simulate processing time
				time.Sleep(time.Microsecond * 100)

				// Read from account B
				balanceB, _ := db.Read(tx, "account_B")

				// Update both accounts (RACE CONDITION!)
				db.Write(tx, "account_A", balanceA-amount)
				db.Write(tx, "account_B", balanceB+amount)

				//unlock the bloc of operations (read account_A, read account_B, write account_A, write account_B)
				db.monitor.releaseBloclock()

				db.Commit(tx)
			}
		}()
	}

	wg.Wait()

	finalA, _ := db.Read(db.BeginTransaction(), "account_A")
	finalB, _ := db.Read(db.BeginTransaction(), "account_B")
	finalTotal := finalA + finalB
	expectedTotal := 2000

	if finalTotal != expectedTotal {
		t.Fatalf("Consistency test failure : ExpectedTotal %d, got %d", expectedTotal, finalTotal)

	}
}
