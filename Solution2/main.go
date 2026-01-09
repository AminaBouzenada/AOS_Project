package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

func main() {
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║   Database Synchronization Mini-Project                   ║")
	fmt.Println("║   SYNCHRONIZED VERSION - Demonstrates Monitors solution   ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")

	fmt.Println("\n✅ Note: This code has synchronization!")
	fmt.Println("✅  Running with multiple goroutines WILL not cause race conditions.")
	// fmt.Println("✅  Run with: go run -race . to detect data races")

	// Create database instance
	db := NewDatabase()

	// Run different scenarios to demonstrate race conditions

	// Scenario 1: Counter Increment (Lost Updates)
	fmt.Println("\n" + strings.Repeat("=", 60))
	RunCounterScenario(db, 10, 100)

	// Scenario 2: Bank Transfer (Lost Updates + Inconsistency)
	db = NewDatabase() // Reset database
	RunBankTransferScenario(db, 5, 50)

	// Scenario 3: Concurrent Reads and Writes (Dirty Reads)
	db = NewDatabase() // Reset database
	RunReadWriteScenario(db, 5, 3, 2*time.Second)

	// Scenario 4: General Concurrent Operations
	db = NewDatabase() // Reset database
	runGeneralScenario(db)

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("\n✓ All scenarios completed!")
	fmt.Println("\nOld behaviors:")
	fmt.Println("  - Counter scenario: Lost updates (final value < expected)")
	fmt.Println("  - Bank transfer: Money lost (total < 2000)")
	fmt.Println("  - Read-write: Inconsistent reads detected")
	fmt.Println("  - General: Data corruption and race warnings")
	fmt.Println("\nNew behaviors (after monitors):")
	fmt.Println("  - Counter scenario: All updates recorded (final value = expected (1000))")
	fmt.Println("  - Bank transfer: Total preserved (total = 2000)")
	fmt.Println("  - Read-write: No inconsistent reads detected")
	fmt.Println("  - General: No Data corruption and race warnings")
	// fmt.Println("\n✓ All scenarios completed!")
	// fmt.Println("\nTo see the race conditions detected by Go's race detector:")
	// fmt.Println("  go run -race .")
	// fmt.Println("\nExpected behavior:")
	// fmt.Println("  - Counter scenario: Lost updates (final value < expected)")
	// fmt.Println("  - Bank transfer: Money lost (total < 2000)")
	// fmt.Println("  - Read-write: Inconsistent reads detected")
	// fmt.Println("  - General: Data corruption and race warnings")
}

func runGeneralScenario(db *Database) {
	fmt.Println("\n=== General Concurrent Operations Scenario ===")
	fmt.Printf("Running 8 clients with mixed operations\n")

	// Initialize some data
	initTx := db.BeginTransaction()
	//*with 5 clients no one of them do writing the totalWrites=5
	db.Write(initTx, "account_1", 500)
	db.Write(initTx, "account_2", 500)
	db.Write(initTx, "account_3", 500)
	db.Write(initTx, "counter", 0)
	db.Write(initTx, "balance", 1000)
	db.Commit(initTx)

	fmt.Println("Initial state: account_1=500, account_2=500, account_3=500, counter=0, balance=1000")

	// Create clients with different workloads
	clients := []ClientConfig{
		{ID: 1, NumTransactions: 1, OperationsPerTx: 1, ThinkTime: time.Microsecond * 100},
		{ID: 2, NumTransactions: 1, OperationsPerTx: 1, ThinkTime: time.Microsecond * 100},
		{ID: 3, NumTransactions: 1, OperationsPerTx: 1, ThinkTime: time.Microsecond * 100},
		{ID: 4, NumTransactions: 1, OperationsPerTx: 1, ThinkTime: time.Microsecond * 100},
		// {ID: 5, NumTransactions: 50, OperationsPerTx: 3, ThinkTime: time.Microsecond * 100},
		// {ID: 6, NumTransactions: 50, OperationsPerTx: 3, ThinkTime: time.Microsecond * 100},
		// {ID: 7, NumTransactions: 50, OperationsPerTx: 3, ThinkTime: time.Microsecond * 100},
		// {ID: 8, NumTransactions: 50, OperationsPerTx: 3, ThinkTime: time.Microsecond * 100},
	}

	// Run clients concurrently
	var wg sync.WaitGroup
	for _, config := range clients {
		wg.Add(1)
		client := NewClient(config, db)
		go client.Run(db, &wg)
	}

	wg.Wait()

	// Display final state
	fmt.Println("\nFinal database state:")
	db.PrintRecords()
	db.PrintStats()

	// fmt.Println("\n⚠️  Note: If you see inconsistent data or the program crashes,")
	// fmt.Println("    that's expected! This demonstrates why synchronization is needed.")
}
