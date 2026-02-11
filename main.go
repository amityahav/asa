package main

import (
	"fmt"
	"log/slog"
	"maps"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type TxStatus int8

const (
	TxInProgress TxStatus = iota
	TxCommited
	TxCanceled
)

type TxModifier struct {
	f  func(interface{}) interface{}
	id int64
}

func (tm *TxModifier) ID() int64 {
	return tm.id
}

func (tm *TxModifier) Apply(val interface{}) interface{} {
	return tm.f(val)
}

type Transaction struct {
	id            int64
	status        atomic.Int32
	storageEngine *StorageEngine
}

type DataEntry struct {
	value       interface{}
	txModifiers []TxModifier // modifiers are ordered by the time of execution
	activeTx    int64        // the in-progress tx that mutated this entry
	mu          sync.RWMutex // used for concurrent access by transactions
}

type StorageEngine struct {
	store     map[string]*DataEntry
	txs       map[int64]*Transaction
	txCounter int64
	mu        sync.RWMutex
	logger    *slog.Logger
}

func NewStorageEngine() *StorageEngine {
	se := StorageEngine{
		store:     make(map[string]*DataEntry),
		txs:       make(map[int64]*Transaction),
		txCounter: 0,
		logger:    slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}

	go se.startCompactionWorker()

	return &se
}

func (e *StorageEngine) put(key string, modifier TxModifier) error {
	e.mu.RLock()
	v, ok := e.store[key]
	e.mu.RUnlock()

	if !ok {
		// Create a new entry.
		e.mu.Lock()
		v, ok = e.store[key]
		if !ok {
			v = &DataEntry{}
		}
		e.store[key] = v
		e.mu.Unlock()
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.activeTx != modifier.ID() {
		if e.isTxStatus(v.activeTx, TxInProgress) {
			// There can be only one transaction in progress, and it will always be the last modifier in txModifiers.
			return fmt.Errorf("write-write conflict occurred for key: %s", key)
		}

		// Rather than eagerly clearing the activeTx field on all associated entries
		// at commit time, we defer the cleanup and resolve it lazily upon the next
		// mutation attempt.
	}

	v.activeTx = modifier.ID()
	v.txModifiers = append(v.txModifiers, modifier)

	return nil
}

func (e *StorageEngine) isTxStatus(id int64, status TxStatus) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	tx, ok := e.txs[id]
	if !ok {
		// The given ID is always a valid transaction ID. If it is not found,
		// it means the janitor has already cleaned it up.
		return false
	}

	return tx.status.Load() == int32(status)
}

func (e *StorageEngine) getTxStatus(id int64) (TxStatus, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	tx, ok := e.txs[id]
	if !ok {
		return 0, false
	}

	return TxStatus(tx.status.Load()), true
}

func (e *StorageEngine) startCompactionWorker() {
	for {
		select {
		case <-time.After(1 * time.Minute):
			err := e.compact()
			if err != nil {
				e.logger.Error(err.Error())
			}
		}
	}
}

func (e *StorageEngine) compact() error {
	var currentEntries []*DataEntry
	e.mu.RLock() // TODO: consider create 2 separate mutexes for txs and store
	for _, entry := range e.store {
		currentEntries = append(currentEntries, entry)
	}
	e.mu.RUnlock()

	type modifierWithTxStatus struct {
		TxModifier
		status TxStatus
	}

	for _, entry := range currentEntries {
		entry.mu.RLock()
		currentModifiers := make([]modifierWithTxStatus, 0, len(entry.txModifiers))
		for _, modifier := range entry.txModifiers {
			status, ok := e.getTxStatus(modifier.ID())
			if !ok {
				continue
			}

			if status == TxInProgress {
				break
			}

			currentModifiers = append(currentModifiers, modifierWithTxStatus{
				TxModifier: modifier,
				status:     status,
			})
		}

		currVal := entry.value // TODO: get a copy
		entry.mu.RUnlock()

		for _, modifier := range currentModifiers {
			if modifier.status != TxCommited {
				continue
			}

			currVal = modifier.Apply(currVal)
		}

		entry.mu.Lock()
		// Assign new base value and truncate all the commited, cancelled modifiers.
		entry.value = currVal
		entry.txModifiers = entry.txModifiers[len(currentModifiers):]
		entry.mu.Unlock()

		e.mu.Lock()
		// TODO: this is wrong!! should remove a tx from map only after applying it in all associated data entries
		// 	can be implemented using a ref count on the transaction
		for _, modifier := range currentModifiers {
			delete(e.txs, modifier.ID())
		}
		e.mu.Unlock()
	}
}

func (e *StorageEngine) Begin() *Transaction {
	e.mu.Lock()
	t := &Transaction{
		id:            e.txCounter,
		storageEngine: e,
	}

	t.status.Store(int32(TxInProgress))

	e.txs[e.txCounter] = t
	e.txCounter++
	e.mu.Unlock()

	return t
}

func (e *StorageEngine) Get(key string) (interface{}, bool) {
	e.mu.RLock()
	v, ok := e.store[key]
	if !ok {
		e.mu.RUnlock()
		return nil, false
	}

	e.mu.RUnlock()

	modifiers := make([]TxModifier, 0)

	v.mu.RLock()
	for _, modifier := range v.txModifiers {
		txID := modifier.ID()

		e.mu.RLock()
		tx, exist := e.txs[txID]
		if !exist {
			// janitor should clean this
			e.mu.RUnlock()
			continue
		}
		e.mu.RUnlock()

		if tx.status.Load() == int32(TxCommited) {
			modifiers = append(modifiers, modifier)
		}
	}

	currVal := v.value // TODO: make sure we get a copy of it
	v.mu.RUnlock()

	for _, modifier := range modifiers {
		currVal = modifier.Apply(currVal)
	}

	return currVal, true
}

func (t *Transaction) Mutate(key string, f func(interface{}) interface{}) error {
	return t.storageEngine.put(key, TxModifier{
		f:  f,
		id: t.id,
	})
}

func (t *Transaction) Commit() {
	t.status.Store(int32(TxCommited))
}

func (t *Transaction) Cancel() {
	t.status.Store(int32(TxCanceled))
}

func DoTransaction(engine *StorageEngine, f func(tx *Transaction) error) (err error) {
	tx := engine.Begin()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v: %s", r, debug.Stack())
			tx.Cancel()
		}
	}()

	err = f(tx)
	if err != nil {
		tx.Cancel()
		return err
	}

	return nil
}
