This repository contains implementation of **sharding technique** for container synchronization, along with synchronization alternatives and some tests for comparative benchmarking.

This article provides the context: [Optimizing Container Synchronization for Frequent Writes](https://dzone.com/articles/optimizing-container-synchronization-for-frequent-writes)

Only source code (C\+\+17) is provided, build it with any IDE/compiler of your liking. It'll require **TBB** and **boost**, but those dependencies can be removed using `NO_REFERENCE_CONTAINERS` macro (see below).

### Contents

**TransactionData.h** contains TransactionData struct, a sample data object to represent container content.

**SyncronizedContainers.h** contains implementations of transaction cache with different approach to synchronization:
- `SimpleSynchronizedCache` uses `std::mutex`
- `CacheWithSharedMutex` uses `std::shared_mutex`
- `ShardedCache` uses sharding over `SimpleSynchronizedCache`

**ReferenceContainers.h** contains implementations of the same transaction cache as in **SyncronizedContainers.h**, but using popular concurrent high-load containers:
- `BoostConcurrentFlatMap` uses `boost::concurrent_flat_map`. Dependency is header-only, code is tested with 1.87.0 version of boost.
- `TbbConcurrentHashMap` uses `tbb::concurrent_hash_map`. Code is tested with 2022.0.0.396 version of Intel oneAPI TBB.

**Tests.cpp** contains benchmark `Test` that simulates multithreaded write-heavy usage of a transaction cache, when operations do not appear simultaneously but more dispersed over time. `main()` contains variety of the test scenarios.

**Auxiliary.h** contains function `simulateWorkload()` to add work simulation (see `SIMULATE_ADDITIONAL_WORKLOAD` below).

### Macros

***SIMULATE_ADDITIONAL_WORKLOAD*** - use it to add 0.1 ms of work simulation to the `pop()` function. This way the benchmark will show the performance of a container that have time consuming operations inside a lock.

***NO_REFERENCE_CONTAINERS*** - use it to exclude library containers, thus the building won't depend on boost and TBB.