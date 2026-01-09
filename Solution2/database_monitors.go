package main

import (
	"fmt"
	"sync"
	"time"
)

// variable mutex lock used to protect shared variables like totalUpdates, totalReads...
var muLock sync.Mutex

// Monitor for the database used for synchronization
type DatabaseMonitor struct {
	mu       sync.Mutex
	cond     *sync.Cond
	locked   map[string]bool // Per-key locks
	BlocLock bool            //block of operations lock.
}

// Record represents a single database record
type Record struct {
	Key       string
	Value     int
	Version   int // Used to detect lost updates
	UpdatedAt time.Time
}

// Transaction represents a database transaction
type Transaction struct {
	ID         int
	StartTime  time.Time
	Operations []string // Log of operations for debugging
}

// Database represents an in-memory key-value database
// WARNING: This implementation has NO synchronization!
// Multiple goroutines accessing this will cause race conditions.
type Database struct {
	records   map[string]*Record
	txCounter int
	stats     Stats
	//solution2
	monitor *DatabaseMonitor
}

// Stats tracks database statistics to detect corruption
type Stats struct {
	TotalReads     int
	TotalWrites    int
	TotalUpdates   int
	LostUpdates    int // Detected when version doesn't increment properly
	DataCorruption int // Detected when data is inconsistent
}

// NewDatabaseMonitor creates new monitor instance
func NewDatabaseMonitor() *DatabaseMonitor {
	db_mntr := &DatabaseMonitor{
		locked:   make(map[string]bool),
		BlocLock: false,
	}
	db_mntr.cond = sync.NewCond(&db_mntr.mu)
	return db_mntr
}

// acquirekey used to allows only one goroutine to access a key record at a time
func (db *DatabaseMonitor) acquireKey(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	for db.locked[key] {
		db.cond.Wait() // Wait until key is free
	}
	db.locked[key] = true
}

// acquireBlocLock used to allows only one goroutine to run a set of operations(read, write...) at a time.
func (db *DatabaseMonitor) acquireBlocLock() {
	db.mu.Lock()
	defer db.mu.Unlock()

	for db.BlocLock {
		db.cond.Wait() //wait until bloc of operations is available.
	}
	db.BlocLock = true
}

// releaseKey used to release the key for other goroutine
func (db *DatabaseMonitor) releaseKey(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.locked[key] = false
	db.cond.Broadcast() // Wake waiting goroutines
}

// releaseBloclock used to release the bloc key or lock for other goroutines.
func (db *DatabaseMonitor) releaseBloclock() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.BlocLock = false
	db.cond.Broadcast() //wake waiting goroutines for bloc lock.
}

// NewDatabase creates a new database instance
func NewDatabase() *Database {
	//solution2
	db_mntr := NewDatabaseMonitor()
	return &Database{
		records:   make(map[string]*Record),
		txCounter: 0,
		//solution2
		monitor: db_mntr,
	}
}

// BeginTransaction starts a new transaction
// RACE CONDITION: txCounter is not protected!
func (db *Database) BeginTransaction() *Transaction {
	//*Protect the counter
	muLock.Lock()
	defer muLock.Unlock()
	db.txCounter++ // UNSAFE: Multiple goroutines can increment simultaneously
	tx := &Transaction{
		ID:         db.txCounter,
		StartTime:  time.Now(),
		Operations: make([]string, 0),
	}
	return tx
}

// Read retrieves a value from the database
// RACE CONDITION: Reading while another goroutine is writing
func (db *Database) Read(tx *Transaction, key string) (int, bool) {
	//key record lock and release.
	db.monitor.acquireKey(key)
	defer db.monitor.releaseKey(key)

	muLock.Lock()
	db.stats.TotalReads++ // UNSAFE: Not atomic
	muLock.Unlock()

	record, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("READ %s: NOT_FOUND", key))
		return 0, false
	}

	// Simulate some processing time to increase likelihood of race conditions
	time.Sleep(time.Microsecond * 10)

	value := record.Value // UNSAFE: Value might change between check and read
	tx.Operations = append(tx.Operations, fmt.Sprintf("READ %s: %d", key, value))
	return value, true
}

// Write creates or updates a record in the database
// RACE CONDITION: Multiple writes to the same key can cause lost updates
func (db *Database) Write(tx *Transaction, key string, value int) {
	//key record lock and release.
	db.monitor.acquireKey(key)
	defer db.monitor.releaseKey(key)

	muLock.Lock()
	db.stats.TotalWrites++ // UNSAFE: Not atomic
	muLock.Unlock()

	existingRecord, exists := db.records[key]

	// Simulate some processing time
	time.Sleep(time.Microsecond * 10)

	if exists {
		// UNSAFE: Another goroutine might update version between read and write
		oldVersion := existingRecord.Version
		existingRecord.Value = value
		existingRecord.Version = oldVersion + 1 // Lost update can happen here!
		existingRecord.UpdatedAt = time.Now()
		tx.Operations = append(tx.Operations, fmt.Sprintf("WRITE %s: %d (v%d)", key, value, existingRecord.Version))
	} else {
		// UNSAFE: Two goroutines might both think the key doesn't exist
		db.records[key] = &Record{
			Key:       key,
			Value:     value,
			Version:   1,
			UpdatedAt: time.Now(),
		}
		tx.Operations = append(tx.Operations, fmt.Sprintf("WRITE %s: %d (new)", key, value))
	}
}

// Update performs a read-modify-write operation
// RACE CONDITION: Classic lost update problem!
func (db *Database) Update(tx *Transaction, key string, delta int) bool {
	//key record lock and release.
	db.monitor.acquireKey(key)
	defer db.monitor.releaseKey(key)

	muLock.Lock()
	db.stats.TotalUpdates++ // UNSAFE: Not atomic
	muLock.Unlock()

	// Read current value
	currentValue, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("UPDATE %s: NOT_FOUND", key))
		return false
	}

	// Simulate some processing time (makes race condition more likely)
	time.Sleep(time.Microsecond * 50)

	// UNSAFE: Another goroutine might have modified the value!
	oldVersion := currentValue.Version
	newValue := currentValue.Value + delta
	currentValue.Value = newValue
	currentValue.Version = oldVersion + 1
	currentValue.UpdatedAt = time.Now()

	tx.Operations = append(tx.Operations, fmt.Sprintf("UPDATE %s: +%d = %d (v%d)", key, delta, newValue, currentValue.Version))
	return true
}

// Delete removes a record from the database
// RACE CONDITION: Concurrent deletes or delete during read
func (db *Database) Delete(tx *Transaction, key string) bool {
	//key record lock and release.
	db.monitor.acquireKey(key)
	defer db.monitor.releaseKey(key)

	_, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("DELETE %s: NOT_FOUND", key))
		return false
	}

	// Simulate some processing time
	time.Sleep(time.Microsecond * 10)

	// UNSAFE: Another goroutine might delete or modify this key
	delete(db.records, key)
	tx.Operations = append(tx.Operations, fmt.Sprintf("DELETE %s: SUCCESS", key))
	return true
}

// Commit finalizes a transaction
func (db *Database) Commit(tx *Transaction) {
	duration := time.Since(tx.StartTime)
	tx.Operations = append(tx.Operations, fmt.Sprintf("COMMIT (duration: %v)", duration))
}

// Abort cancels a transaction
func (db *Database) Abort(tx *Transaction) {
	duration := time.Since(tx.StartTime)
	tx.Operations = append(tx.Operations, fmt.Sprintf("ABORT (duration: %v)", duration))
}

// GetStats returns current database statistics
// RACE CONDITION: Stats are being read while being modified
func (db *Database) GetStats() Stats {
	return db.stats // UNSAFE: Struct copy is not atomic
}

// VerifyIntegrity checks for data corruption
// This helps demonstrate that race conditions occurred
func (db *Database) VerifyIntegrity(expectedValues map[string]int) (bool, []string) {
	errors := make([]string, 0)

	for key, expectedValue := range expectedValues {
		record, exists := db.records[key]
		if !exists {
			errors = append(errors, fmt.Sprintf("Key %s missing (expected %d)", key, expectedValue))
			continue
		}

		if record.Value != expectedValue {
			errors = append(errors, fmt.Sprintf("Key %s has value %d (expected %d)", key, record.Value, expectedValue))
			db.stats.DataCorruption++
		}
	}

	return len(errors) == 0, errors
}

// PrintStats displays database statistics
func (db *Database) PrintStats() {
	stats := db.GetStats()
	fmt.Println("\n=== Database Statistics ===")
	fmt.Printf("Total Reads:     %d\n", stats.TotalReads)
	fmt.Printf("Total Writes:    %d\n", stats.TotalWrites)
	fmt.Printf("Total Updates:   %d\n", stats.TotalUpdates)
	fmt.Printf("Lost Updates:    %d\n", stats.LostUpdates)
	fmt.Printf("Data Corruption: %d\n", stats.DataCorruption)
	fmt.Println("===========================")
}

// GetRecordCount returns the number of records
// RACE CONDITION: Map length can change during iteration
func (db *Database) GetRecordCount() int {
	return len(db.records) // UNSAFE: Map access not synchronized
}

// PrintRecords displays all records (for debugging)
// RACE CONDITION: Iterating over map while it's being modified
func (db *Database) PrintRecords() {
	fmt.Println("\n=== Database Records ===")
	for key, record := range db.records { // UNSAFE: Concurrent map iteration
		fmt.Printf("%s: value=%d, version=%d, updated=%v\n",
			key, record.Value, record.Version, record.UpdatedAt.Format("15:04:05.000"))
	}
	fmt.Println("========================")
}
