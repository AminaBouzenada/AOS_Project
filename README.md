🧪tests instructions:

   1- To compile and check CRUD_test, consistancy_test, stress_test and concurrency_test, we used: 
        go test ./...
        
      Command output: 
        ok      database-sync-unsynchronized(all tests are compiled and passed).
        
   2- Check for race conditions, we used: 
        go test -race -v ./...
        
      Command output: 
        === RUN   TestCRUDoperations
        --- PASS: TestCRUDoperations (0.00s)
        === RUN   TestConcurrency
        --- PASS: TestConcurrency (0.81s)
        === RUN   TestConsistencyByBankTransfer
        --- PASS: TestConsistencyByBankTransfer (0.86s)
        === RUN   TestStress
        --- PASS: TestStress (31.49s)
        PASS
        ok      database-sync-unsynchronized    35.176s

   3- Run benchmarks:
       go test -bench=. -benchmem
      

    or:


    1- to run tests: go run

    2- results:
    PASS
    ok      database-sync-unsynchronized    3.501s

    3- go test -bench .



