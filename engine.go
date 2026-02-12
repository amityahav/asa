package main

import (
	"errors"
	"fmt"
	"log/slog"
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

type Cloner[T any] interface {
	Clone() T
}

type TxModifier[T Cloner[T]] struct {
	f  func(T) (T, error)
	tx *Transaction[T]
}

func (tm *TxModifier[T]) ID() int64 {
	return tm.tx.id
}

func (tm *TxModifier[T]) Apply(val T) (T, error) {
	return tm.f(val)
}

type Transaction[T Cloner[T]] struct {
	id            int64
	status        atomic.Int32
	storageEngine *StorageEngine[T]
}

type DataEntry[T Cloner[T]] struct {
	value       T
	txModifiers []TxModifier[T] // modifiers are ordered by the time of execution
	activeTx    int64           // the in-progress tx that mutated this entry
	mu          sync.RWMutex    // used for concurrent access by transactions
}

type StorageEngine[T Cloner[T]] struct {
	store     map[string]*DataEntry[T]
	txCounter int64 // incremented id for transactions
	mu        sync.RWMutex
	logger    *slog.Logger
}

func NewStorageEngine[T Cloner[T]]() *StorageEngine[T] {
	se := StorageEngine[T]{
		store:     make(map[string]*DataEntry[T]),
		txCounter: 0,
		logger:    slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}

	go se.startCompactionWorker()

	return &se
}

func (e *StorageEngine[T]) put(key string, modifier TxModifier[T]) error {
	e.mu.RLock()
	v, ok := e.store[key]
	e.mu.RUnlock()

	if !ok {
		// Create a new entry.
		e.mu.Lock()
		v, ok = e.store[key]
		if !ok {
			v = &DataEntry[T]{}
		}
		e.store[key] = v
		e.mu.Unlock()
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.activeTx != modifier.ID() {
		if modifier.tx.status.Load() == int32(TxInProgress) {
			// There can be only one transaction in progress, and it will always be the last modifier in txModifiers.
			return fmt.Errorf("write-write conflict detected for key: %s", key)
		}

		// Rather than eagerly clearing the activeTx field on all associated entries
		// at commit time, we defer the cleanup and resolve it lazily upon the next
		// mutation attempt.
	}

	v.activeTx = modifier.ID()
	v.txModifiers = append(v.txModifiers, modifier)

	return nil
}

func (e *StorageEngine[T]) startCompactionWorker() {
	for {
		select {
		case <-time.After(30 * time.Second):
			e.compact()
		}
	}
}

func (e *StorageEngine[T]) compact() {
	var currentEntries []*DataEntry[T]
	e.mu.RLock()
	for _, entry := range e.store {
		currentEntries = append(currentEntries, entry)
	}
	e.mu.RUnlock()

	for _, entry := range currentEntries {
		entry.mu.RLock()
		currentModifiers := make([]TxModifier[T], 0, len(entry.txModifiers))
		for _, modifier := range entry.txModifiers {
			if modifier.tx.status.Load() == int32(TxInProgress) {
				break
			}

			currentModifiers = append(currentModifiers, modifier)
		}

		currVal := entry.value.Clone()
		entry.mu.RUnlock()

		var err error
		for _, modifier := range currentModifiers {
			if modifier.tx.status.Load() == int32(TxCanceled) {
				continue
			}

			currVal, err = modifier.Apply(currVal)
			if err != nil {
				break
			}
		}

		if err != nil {
			e.logger.Error(err.Error())
			continue
		}

		entry.mu.Lock()
		// Assign new base value and truncate all the commited, cancelled modifiers.
		entry.value = currVal
		entry.txModifiers = entry.txModifiers[len(currentModifiers):]
		entry.mu.Unlock()
	}
}

func (e *StorageEngine[T]) Begin() *Transaction[T] {
	e.mu.Lock()
	currId := e.txCounter
	e.txCounter++
	e.mu.Unlock()

	t := &Transaction[T]{
		id:            currId,
		storageEngine: e,
	}

	t.status.Store(int32(TxInProgress))

	return t
}

func (e *StorageEngine[T]) Get(key string) (T, error) {
	var zero T

	e.mu.RLock()
	v, ok := e.store[key]
	if !ok {
		e.mu.RUnlock()
		return zero, ErrKeyNotFound
	}
	e.mu.RUnlock()

	modifiers := make([]TxModifier[T], 0)

	v.mu.RLock()
	for _, modifier := range v.txModifiers {
		if modifier.tx.status.Load() == int32(TxCommited) {
			modifiers = append(modifiers, modifier)
		}
	}

	currVal := v.value.Clone()
	v.mu.RUnlock()

	var err error
	for _, modifier := range modifiers {
		currVal, err = modifier.Apply(currVal)
		if err != nil {
			return zero, err
		}
	}

	return currVal, nil
}

func (t *Transaction[T]) Mutate(key string, f func(T) (T, error)) error {
	err := t.storageEngine.put(key, TxModifier[T]{
		f:  f,
		tx: t,
	})
	if err != nil {
		t.Cancel()
	}

	return err
}

func (t *Transaction[T]) Commit() {
	t.status.Store(int32(TxCommited))
}

func (t *Transaction[T]) Cancel() {
	if t.status.Load() == int32(TxCommited) {
		// no-op in case the transaction has commited.
		return
	}

	t.status.Store(int32(TxCanceled))
}

func DoTransaction[T Cloner[T]](engine *StorageEngine[T], f func(tx *Transaction[T]) error) (err error) {
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

var ErrKeyNotFound = errors.New("key not found")
