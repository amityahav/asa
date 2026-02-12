package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// INode is a simplified struct representing a filesystem inode.
// For directories, Children holds the names of entries in that directory.
// Parent is the key of the containing directory (for files).
type INode struct {
	Name     string
	IsDir    bool
	Parent   string
	Children []string
}

func (n INode) Clone() INode {
	children := make([]string, len(n.Children))
	copy(children, n.Children)
	return INode{
		Name:     n.Name,
		IsDir:    n.IsDir,
		Parent:   n.Parent,
		Children: children,
	}
}

func TestMoveFileFromDirectoryAToDirectoryB(t *testing.T) {
	engine := NewStorageEngine[INode](nil)

	dirA := INode{Name: "A", IsDir: true, Children: []string{"x", "y"}}
	dirB := INode{Name: "B", IsDir: true, Children: []string{}}
	fileX := INode{Name: "x", IsDir: false, Parent: "A"}

	// Setup
	err := DoTransaction(engine, func(tx *Transaction[INode]) error {
		if err := tx.Mutate("A", func(_ INode) (INode, error) { return dirA, nil }); err != nil {
			return err
		}
		if err := tx.Mutate("B", func(_ INode) (INode, error) { return dirB, nil }); err != nil {
			return err
		}
		if err := tx.Mutate("x", func(_ INode) (INode, error) { return fileX, nil }); err != nil {
			return err
		}
		return nil
	})

	// move file "x" from A to B:
	// (remove entry from A, add to B, update x's parent to B)
	err = DoTransaction(engine, func(tx *Transaction[INode]) error {
		// Remove "x" from directory A
		if err := tx.Mutate("A", removeFromDir("x")); err != nil {
			return err
		}
		// Add "x" to directory B
		if err := tx.Mutate("B", addToDir("x")); err != nil {
			return err
		}
		// Update file x's parent from A to B
		if err := tx.Mutate("x", func(n INode) (INode, error) {
			n.Parent = "B"
			return n, nil
		}); err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)

	// Assert: A no longer has "x", B has "x"
	a, err := engine.Get("A")
	require.NoError(t, err)
	require.NotContains(t, a.Children, "x")

	b, err := engine.Get("B")
	require.NoError(t, err)
	require.Contains(t, b.Children, "x")

	// File x's parent was A, now points to B
	x, err := engine.Get("x")
	require.NoError(t, err)
	require.Equal(t, "B", x.Parent)
}

// removeFromDir returns a mutator that removes the given entry name from an INode's Children.
func removeFromDir(name string) func(INode) (INode, error) {
	return func(n INode) (INode, error) {
		children := make([]string, 0, len(n.Children))
		for _, c := range n.Children {
			if c != name {
				children = append(children, c)
			}
		}
		n.Children = children
		return n, nil
	}
}

// addToDir returns a mutator that appends the given entry name to an INode's Children.
func addToDir(name string) func(INode) (INode, error) {
	return func(n INode) (INode, error) {
		n.Children = append(n.Children, name)
		return n, nil
	}
}

// countModifiers returns the number of tx modifiers for key on the engine (same package: reads store + txModifiers).
func countModifiers(engine *StorageEngine[INode], key string) int {
	engine.mu.RLock()
	v, ok := engine.store[key]
	engine.mu.RUnlock()
	if !ok {
		return -1
	}
	v.mu.RLock()
	n := len(v.txModifiers)
	v.mu.RUnlock()
	return n
}

func TestTwoConcurrentMovesOneFails(t *testing.T) {
	engine := NewStorageEngine[INode](nil)

	dirA := INode{Name: "A", IsDir: true, Children: []string{"x", "y"}}
	dirB := INode{Name: "B", IsDir: true, Children: []string{}}
	dirC := INode{Name: "C", IsDir: true, Children: []string{}}
	fileX := INode{Name: "x", IsDir: false, Parent: "A"}

	// Setup: A, B, C and x (in A)
	err := DoTransaction(engine, func(tx *Transaction[INode]) error {
		require.NoError(t, tx.Mutate("A", func(_ INode) (INode, error) { return dirA, nil }))
		require.NoError(t, tx.Mutate("B", func(_ INode) (INode, error) { return dirB, nil }))
		require.NoError(t, tx.Mutate("C", func(_ INode) (INode, error) { return dirC, nil }))
		require.NoError(t, tx.Mutate("x", func(_ INode) (INode, error) { return fileX, nil }))
		return nil
	})
	require.NoError(t, err)

	// Two transactions using Begin/Mutate directly: one moves x A→B, one moves x A→C
	txToB := engine.Begin()
	txToC := engine.Begin()

	// Both try to mutate A first (remove x). Whichever runs second hits write-write conflict.
	err1 := txToB.Mutate("A", removeFromDir("x"))
	require.NoError(t, err1)

	err2 := txToC.Mutate("A", removeFromDir("x"))
	require.Error(t, err2)
	require.Contains(t, err2.Error(), "write-write conflict")

	// txToC is canceled by Mutate on error. Complete txToB: add x to B, update x's parent, commit.
	require.NoError(t, txToB.Mutate("B", addToDir("x")))
	require.NoError(t, txToB.Mutate("x", func(n INode) (INode, error) {
		n.Parent = "B"
		return n, nil
	}))
	txToB.Commit()

	// Final state: x moved to B; C unchanged
	a, _ := engine.Get("A")
	require.NotContains(t, a.Children, "x")
	b, _ := engine.Get("B")
	require.Contains(t, b.Children, "x")
	c, _ := engine.Get("C")
	require.NotContains(t, c.Children, "x")
	x, _ := engine.Get("x")
	require.Equal(t, "B", x.Parent)
}

func TestCompaction(t *testing.T) {
	engine := NewStorageEngine[INode](nil)

	// Commit initial value for "dir": Children ["a"]
	err := DoTransaction(engine, func(tx *Transaction[INode]) error {
		return tx.Mutate("dir", func(_ INode) (INode, error) {
			return INode{Name: "dir", IsDir: true, Children: []string{"a"}}, nil
		})
	})
	require.NoError(t, err)

	// Commit a second modifier: add "b" to dir
	err = DoTransaction(engine, func(tx *Transaction[INode]) error {
		return tx.Mutate("dir", addToDir("b"))
	})
	require.NoError(t, err)

	// Before compaction, Get applies committed modifiers and returns correct value
	before, err := engine.Get("dir")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, before.Children)

	// "dir" has two committed modifiers before compaction
	require.Equal(t, 2, countModifiers(engine, "dir"), "two modifiers before compact")

	// Run compaction: committed modifiers are folded into base value and truncated
	engine.compact()

	// After compaction, Get still returns the same value (now from base + any remaining modifiers)
	after, err := engine.Get("dir")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, after.Children)

	// DataEntry's txModifiers must be truncated (committed modifiers removed)
	require.Equal(t, 0, countModifiers(engine, "dir"), "tx modifiers must be truncated after compaction")

	// Another committed mutation and compact: ensures compaction doesn't break future writes
	err = DoTransaction(engine, func(tx *Transaction[INode]) error {
		return tx.Mutate("dir", addToDir("c"))
	})
	require.NoError(t, err)
	require.Equal(t, 1, countModifiers(engine, "dir"))
	engine.compact()
	final, err := engine.Get("dir")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, final.Children)
	require.Equal(t, 0, countModifiers(engine, "dir"), "tx modifiers truncated again after second compaction")
}

func TestGetDoesNotSeeUncommittedModifications(t *testing.T) {
	engine := NewStorageEngine[INode](nil)

	// Setup: commit A with Children ["x", "y"]
	err := DoTransaction(engine, func(tx *Transaction[INode]) error {
		return tx.Mutate("A", func(_ INode) (INode, error) {
			return INode{Name: "A", IsDir: true, Children: []string{"x", "y"}}, nil
		})
	})
	require.NoError(t, err)

	tx := engine.Begin()
	// Mutate A (remove "x") but do not commit yet
	err = tx.Mutate("A", removeFromDir("x"))
	require.NoError(t, err)

	// Get in the middle of tx: must not see the removal (only committed state)
	midTx, err := engine.Get("A")
	require.NoError(t, err)
	require.Contains(t, midTx.Children, "x", "Get must not see uncommitted modifications")
	require.Contains(t, midTx.Children, "y")

	tx.Commit()

	// After commit, Get sees the modification
	afterCommit, err := engine.Get("A")
	require.NoError(t, err)
	require.NotContains(t, afterCommit.Children, "x")
	require.Contains(t, afterCommit.Children, "y")
}

func TestCanceledTxModifierStillCounted(t *testing.T) {
	engine := NewStorageEngine[INode](nil)

	dirA := INode{Name: "A", IsDir: true, Children: []string{"x", "y"}}
	dirB := INode{Name: "B", IsDir: true, Children: []string{}}

	err := DoTransaction(engine, func(tx *Transaction[INode]) error {
		require.NoError(t, tx.Mutate("A", func(_ INode) (INode, error) { return dirA, nil }))
		require.NoError(t, tx.Mutate("B", func(_ INode) (INode, error) { return dirB, nil }))
		return nil
	})
	require.NoError(t, err)

	engine.compact()

	// TxB holds B (in progress)
	txB := engine.Begin()
	require.NoError(t, txB.Mutate("B", addToDir("z")))

	// TxA modifies A, then tries to modify B and fails (B has TxB in progress) -> TxA is canceled
	txA := engine.Begin()
	require.NoError(t, txA.Mutate("A", removeFromDir("x")))
	err = txA.Mutate("B", addToDir("w"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "write-write conflict")

	// A has one modifier (from canceled TxA); count must include it
	require.Equal(t, 1, countModifiers(engine, "A"), "A must have 1 modifier (canceled tx)")

	// Another tx modifies A and commits
	err = DoTransaction(engine, func(tx *Transaction[INode]) error {
		return tx.Mutate("A", addToDir("z"))
	})
	require.NoError(t, err)

	// A now has two modifiers: canceled TxA + committed TxOther. Count must include the canceled one.
	require.Equal(t, 2, countModifiers(engine, "A"), "number of modifiers must include the cancelled tx")

	// Get(A) only applies committed modifier -> base ["x","y"] + add "z" -> ["x","y","z"]
	a, err := engine.Get("A")
	require.NoError(t, err)
	require.Equal(t, []string{"x", "y", "z"}, a.Children)

	txB.Cancel()
}
