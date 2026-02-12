# asa

A generic, in-memory storage engine with transactions and compaction. Values are updated by applying ordered modifiers; only committed modifiers are visible to `Get`, and a background compactor folds them into the base value and truncates the modifier list.

## Requirements

- Go 1.25+

## Overview

- **Generic storage:** `StorageEngine[T]` where `T` must implement `Cloner[T]` (method `Clone() T`).
- **Key-value model:** Each key maps to a `DataEntry[T]` with a base value and a list of transaction modifiers.
- **Transactions:** `Begin()` starts a transaction; `Mutate(key, f)` adds a modifier `f(value) -> (newValue, error)`; `Commit()` or `Cancel()` ends it. Only committed modifiers are applied when reading.
- **Visibility:** `Get(key)` applies all **committed** modifiers in order. Uncommitted or canceled modifiers are not applied.
- **Write-write conflict:** At most one transaction may have a given key “in progress” at a time. If another transaction tries to mutate that key while it is held by an in-progress transaction, the call returns a write-write conflict error and the caller’s transaction is canceled.
- **Compaction:** A background goroutine runs at a configurable interval (default 30s). It folds all committed (and skips canceled) modifiers into the base value and truncates the modifier list for each key. In-progress modifiers are left in the list.

## Usage

```go
// Default compaction interval (30s)
engine := NewStorageEngine[MyType](nil)

// Custom compaction interval
engine := NewStorageEngine[MyType](&EngineConfig{CompactionInterval: 10 * time.Second})

// Option 1: helper (commit on success, cancel on error/panic)
err := DoTransaction(engine, func(tx *Transaction[MyType]) error {
    if err := tx.Mutate("key", func(v MyType) (MyType, error) {
        // compute new value from v
        return newV, nil
    }); err != nil {
        return err
    }
    return nil
})

// Option 2: manual
tx := engine.Begin()
_ = tx.Mutate("key", ...)
tx.Commit()   // or tx.Cancel()

val, err := engine.Get("key")
```

## Running tests

```bash
go test -v ./...
```

Tests include: move file between directories (setup + move tx), two concurrent moves where one fails with write-write conflict, compaction and modifier truncation, Get not seeing uncommitted changes, and that a canceled transaction’s modifier is still counted until compaction.
