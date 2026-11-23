package bolt_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/boltdb/bolt"
)

// Ensure that deleting every key from inside ForEach still visits every key.
// Iteration and deletion must not share a cursor: removing the current key
// shifts the following keys left, so a shared cursor skips the key right
// after each deleted one and leaves roughly half of the data behind.
func TestBucket_ForEach_DeleteAll(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	const n = 20
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("sessions"))
		if err != nil {
			return err
		}
		expected := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			key := itob(uint64(i + 1))
			if err := b.Put(key, []byte("session")); err != nil {
				return err
			}
			expected = append(expected, key)
		}

		visited := make([][]byte, 0, n)
		if err := b.ForEach(func(k, v []byte) error {
			visited = append(visited, cloneTestBytes(k))
			return b.Delete(k)
		}); err != nil {
			return err
		}

		if len(visited) != n {
			t.Fatalf("visited %d of %d keys; every key must be seen", len(visited), n)
		}
		for i, key := range expected {
			if !bytes.Equal(visited[i], key) {
				t.Fatalf("visited[%d] = %x; want %x (sorted order)", i, visited[i], key)
			}
		}

		remaining := 0
		if err := b.ForEach(func(k, v []byte) error {
			remaining++
			return nil
		}); err != nil {
			return err
		}
		if remaining != 0 {
			t.Fatalf("%d keys remain after deleting during ForEach", remaining)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure deletions during ForEach visit every key once the bucket spans
// multiple B+tree pages, where the cursor has to move between leaf pages.
func TestBucket_ForEach_DeleteAll_MultiPage(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	const n = 50000
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("sessions"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := b.Put(itob(uint64(i+1)), make([]byte, 128)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("sessions"))
		if b.Stats().LeafPageN < 2 {
			t.Fatalf("test setup invalid: expected multiple leaf pages, got %d", b.Stats().LeafPageN)
		}
		visited := 0
		if err := b.ForEach(func(k, v []byte) error {
			visited++
			return b.Delete(k)
		}); err != nil {
			return err
		}
		if visited != n {
			t.Fatalf("visited %d of %d keys; every key must be seen", visited, n)
		}

		remaining := 0
		if err := b.ForEach(func(k, v []byte) error {
			remaining++
			return nil
		}); err != nil {
			return err
		}
		if remaining != 0 {
			t.Fatalf("%d keys remain after deleting during ForEach", remaining)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure deleting every primary-session key while iterating also lets the
// caller clean up a secondary index bucket without skipping either side.
func TestBucket_ForEach_DeleteAll_WithSecondaryIndex(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	const n = 100
	if err := db.Update(func(tx *bolt.Tx) error {
		sessions, err := tx.CreateBucket([]byte("sessions"))
		if err != nil {
			return err
		}
		index, err := tx.CreateBucket([]byte("sessions_by_expiry"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			id := itob(uint64(i + 1))
			expiry := itob(uint64(1000 + i))
			if err := sessions.Put(id, expiry); err != nil {
				return err
			}
			if err := index.Put(append(cloneTestBytes(expiry), id...), id); err != nil {
				return err
			}
		}

		visited := 0
		if err := sessions.ForEach(func(id, expiry []byte) error {
			visited++
			indexKey := append(cloneTestBytes(expiry), id...)
			if err := index.Delete(indexKey); err != nil {
				return err
			}
			return sessions.Delete(id)
		}); err != nil {
			return err
		}
		if visited != n {
			t.Fatalf("visited %d of %d sessions; every session must be seen", visited, n)
		}

		sessionCount, indexCount := 0, 0
		if err := sessions.ForEach(func(k, v []byte) error {
			sessionCount++
			return nil
		}); err != nil {
			return err
		}
		if err := index.ForEach(func(k, v []byte) error {
			indexCount++
			return nil
		}); err != nil {
			return err
		}
		if sessionCount != 0 || indexCount != 0 {
			t.Fatalf("cleanup incomplete: %d sessions, %d index entries remain", sessionCount, indexCount)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure a key deleted by an earlier callback is not revisited and that keys
// which survive the iteration are still each visited once.
func TestBucket_ForEach_DeleteEveryOther(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	const n = 20
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := b.Put(itob(uint64(i+1)), nil); err != nil {
				return err
			}
		}

		visited := 0
		if err := b.ForEach(func(k, v []byte) error {
			visited++
			if binary.BigEndian.Uint64(k)%2 == 0 {
				return b.Delete(k)
			}
			return nil
		}); err != nil {
			return err
		}
		if visited != n {
			t.Fatalf("visited %d of %d keys; every original key must be seen", visited, n)
		}

		remaining := 0
		if err := b.ForEach(func(k, v []byte) error {
			if binary.BigEndian.Uint64(k)%2 == 0 {
				t.Fatalf("even key %x should have been deleted", k)
			}
			remaining++
			return nil
		}); err != nil {
			return err
		}
		if remaining != n/2 {
			t.Fatalf("expected %d surviving keys, got %d", n/2, remaining)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure nested bucket entries (reported with a nil value) and regular keys
// are both visited exactly once when regular keys are deleted during
// iteration. The bucket entry itself must remain untouched.
func TestBucket_ForEach_DeleteAlongsideNestedBucket(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucket([]byte("root"))
		if err != nil {
			return err
		}
		if _, err := root.CreateBucket([]byte("nested")); err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			if err := root.Put(itob(uint64(i+1)), []byte("v")); err != nil {
				return err
			}
		}

		var seenKeys [][]byte
		sawNested := false
		if err := root.ForEach(func(k, v []byte) error {
			if v == nil {
				if !bytes.Equal(k, []byte("nested")) {
					t.Fatalf("unexpected bucket entry: %x", k)
				}
				sawNested = true
				return nil
			}
			seenKeys = append(seenKeys, cloneTestBytes(k))
			return root.Delete(k)
		}); err != nil {
			return err
		}

		if !sawNested {
			t.Fatal("nested bucket entry was not visited")
		}
		if len(seenKeys) != 10 {
			t.Fatalf("visited %d of 10 regular keys", len(seenKeys))
		}
		if root.Bucket([]byte("nested")) == nil {
			t.Fatal("nested bucket must survive the iteration")
		}

		remaining := 0
		if err := root.ForEach(func(k, v []byte) error {
			remaining++
			return nil
		}); err != nil {
			return err
		}
		if remaining != 1 {
			t.Fatalf("expected only the nested bucket to remain, got %d entries", remaining)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func cloneTestBytes(b []byte) []byte {
	clone := make([]byte, len(b))
	copy(clone, b)
	return clone
}
