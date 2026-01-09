package main

import "testing"

//TestCRUDoperations tests that CRUD fuctions (read, write, update and delete) works as expected.
func TestCRUDoperations(t *testing.T) {
	db := NewDatabase()

	tx1 := db.BeginTransaction()
	db.Write(tx1, "A", 10)
	db.Update(tx1, "A", 10)
	db.Write(tx1, "B", 20)
	db.Commit(tx1)

	tx2 := db.BeginTransaction()
	A, _ := db.Read(tx2, "A")
	B, _ := db.Read(tx2, "B")
	db.Commit(tx2)

	if A != B {
		t.Fatalf("expected A=20, B=20; got A=%d, B=%d", A, B)
	}
}
