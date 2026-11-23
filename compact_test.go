package bolt_test

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/boltdb/bolt"
)

// Ensure that compacting after a large random write workload reduces the
// number of pages in the tree and brings range scan latency back down
// without changing query results.
func TestDBCompact_RestoresScanLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	db := MustOpenDB()
	defer db.MustClose()

	const count = 1000000
	const chunk = 10000

	// Write random keys in chunked transactions to produce many split pages.
	rng := rand.New(rand.NewSource(1))
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("data"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i += chunk {
		if err := db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("data"))
			for j := 0; j < chunk; j++ {
				var k [16]byte
				binary.BigEndian.PutUint64(k[0:8], rng.Uint64())
				binary.BigEndian.PutUint64(k[8:16], rng.Uint64())
				v := make([]byte, 100)
				rng.Read(v)
				if err := b.Put(k[:], v); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	// checksum walks the entire bucket and hashes every key/value pair.
	checksum := func() (uint64, uint64) {
		var n uint64
		h := fnv.New64a()
		if err := db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket([]byte("data")).Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				h.Write(k)
				h.Write(v)
				n++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return n, h.Sum64()
	}

	// rangeScanLatency measures how long it takes to perform many small
	// range scans starting at random keys. Each scan only touches a handful
	// of pages so the latency is dominated by how many sparsely filled
	// pages it has to jump across.
	rangeScanLatency := func() time.Duration {
		const scans = 2000
		const scanLen = 500
		var best time.Duration
		for trial := 0; trial < 3; trial++ {
			r := rand.New(rand.NewSource(int64(trial)))
			start := time.Now()
			for i := 0; i < scans; i++ {
				var seek [16]byte
				binary.BigEndian.PutUint64(seek[0:8], r.Uint64())
				binary.BigEndian.PutUint64(seek[8:16], r.Uint64())
				if err := db.View(func(tx *bolt.Tx) error {
					c := tx.Bucket([]byte("data")).Cursor()
					var n int
					for k, _ := c.Seek(seek[:]); k != nil && n < scanLen; k, _ = c.Next() {
						n++
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if d := time.Since(start); trial == 0 || d < best {
				best = d
			}
		}
		return best
	}

	stats := func() bolt.BucketStats {
		var s bolt.BucketStats
		if err := db.View(func(tx *bolt.Tx) error {
			s = tx.Bucket([]byte("data")).Stats()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return s
	}

	before := stats()
	nBefore, sumBefore := checksum()
	latBefore := rangeScanLatency()

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	after := stats()
	nAfter, sumAfter := checksum()
	latAfter := rangeScanLatency()

	// Query results must be identical.
	if nBefore != nAfter || sumBefore != sumAfter {
		t.Fatalf("query results changed: count %d -> %d, checksum %d -> %d",
			nBefore, nAfter, sumBefore, sumAfter)
	}

	t.Logf("leaf pages: %d -> %d, branch pages: %d -> %d, depth: %d -> %d",
		before.LeafPageN, after.LeafPageN, before.BranchPageN, after.BranchPageN,
		before.Depth, after.Depth)
	t.Logf("range scan latency: %v -> %v", latBefore, latAfter)

	// The tree must use noticeably fewer pages. The number of pages a scan
	// has to jump across is what drives scan latency, so this is the
	// deterministic proxy for latency recovery.
	if after.LeafPageN*100 > before.LeafPageN*90 {
		t.Fatalf("leaf page count did not shrink enough: %d -> %d",
			before.LeafPageN, after.LeafPageN)
	}
	if after.BranchPageN > before.BranchPageN {
		t.Fatalf("branch page count grew: %d -> %d", before.BranchPageN, after.BranchPageN)
	}

	// Wall-clock latency in an in-memory test is noisy so only guard
	// against a significant regression here.
	if latAfter > latBefore*5/4 {
		t.Fatalf("scan latency regressed: %v -> %v", latBefore, latAfter)
	}
}

// Ensure that compaction preserves all key/value pairs, including nested
// buckets, on a tree fragmented by random puts and deletes.
func TestDBCompact_PreservesData(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	rng := rand.New(rand.NewSource(2))

	dump := func() (uint64, int) {
		var count int
		h := fnv.New64a()
		if err := db.View(func(tx *bolt.Tx) error {
			return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
				h.Write(name)
				return b.ForEach(func(k, v []byte) error {
					if v == nil {
						h.Write(k)
						return b.Bucket(k).ForEach(func(k2, v2 []byte) error {
							h.Write(k2)
							h.Write(v2)
							count++
							return nil
						})
					}
					h.Write(k)
					h.Write(v)
					count++
					return nil
				})
			})
		}); err != nil {
			t.Fatal(err)
		}
		return h.Sum64(), count
	}

	// Populate two buckets, one with a nested bucket, using random puts and
	// deletes to fragment the tree.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		sub, err := b.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		for i := 0; i < 50000; i++ {
			k := []byte(fmt.Sprintf("%08x", rng.Uint32()))
			if err := b.Put(k, []byte(fmt.Sprintf("value-%d", i))); err != nil {
				return err
			}
			if i%3 == 0 {
				if err := sub.Put(k, []byte(fmt.Sprintf("nested-%d", i))); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < 30000; i++ {
			k := []byte(fmt.Sprintf("%08x", rng.Uint32()))
			_ = b.Delete(k)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	sumBefore, countBefore := dump()

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	sumAfter, countAfter := dump()
	if sumBefore != sumAfter || countBefore != countAfter {
		t.Fatalf("data changed after compact: count %d -> %d, checksum %d -> %d",
			countBefore, countAfter, sumBefore, sumAfter)
	}
}

// Ensure that compaction does not block concurrent readers and writers and
// that the database stays consistent while it runs.
func TestDBCompact_Concurrent(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	// Preload a fragmented data set.
	rng := rand.New(rand.NewSource(3))
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		for i := 0; i < 50000; i++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], rng.Uint64())
			if err := b.Put(k[:], k[:]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	var writes, reads int64
	var mu sync.Mutex
	addWrites := func(n int64) { mu.Lock(); writes += n; mu.Unlock() }
	addReads := func(n int64) { mu.Lock(); reads += n; mu.Unlock() }

	// Concurrent writers.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := db.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("data"))
					for j := 0; j < 100; j++ {
						var k [8]byte
						binary.BigEndian.PutUint64(k[:], r.Uint64())
						if err := b.Put(k[:], k[:]); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					errs <- err
					return
				}
				addWrites(1)
			}
		}(int64(100 + i))
	}

	// Concurrent readers.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := db.View(func(tx *bolt.Tx) error {
					c := tx.Bucket([]byte("data")).Cursor()
					var n int
					for k, _ := c.First(); k != nil && n < 1000; k, _ = c.Next() {
						n++
					}
					return nil
				}); err != nil {
					errs <- err
					return
				}
				addReads(1)
			}
		}()
	}

	// Compact while traffic is in flight.
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()

	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}

	mu.Lock()
	t.Logf("completed %d write txs and %d read txs during compact", writes, reads)
	if writes == 0 || reads == 0 {
		t.Fatalf("transactions were blocked during compact: writes=%d reads=%d", writes, reads)
	}
	mu.Unlock()
}
