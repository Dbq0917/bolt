package bolt

const (
	// compactFillPercent is the target fill percentage used when compaction
	// merges adjacent nodes. Nodes are combined as long as their combined
	// size stays below this percentage of a page.
	compactFillPercent = 0.9

	// compactMaxWorkPerTx bounds how many leaf pages are examined and how
	// many nodes are merged within a single transaction during compaction.
	// Keeping each transaction small ensures concurrent readers and writers
	// are not blocked for long.
	compactMaxWorkPerTx = 256
)

// Compact reclaims space in the b+tree by merging adjacent, sparsely filled
// pages. Random write and delete workloads split pages and leave the tree
// with many half-empty pages, which slows down range scans. Compact walks
// every bucket and merges neighboring nodes that fit together into a single
// page, restoring scan locality without changing any key/value pairs.
//
// The work is performed as a series of short write transactions so
// long-running read transactions are never blocked and concurrent writers
// only wait for a single small chunk at a time. It is safe to call Compact
// while other goroutines are reading from or writing to the database.
//
// Compact must not be called from within a transaction. It reduces the
// number of pages used by the tree but does not shrink the database file;
// freed pages are returned to the freelist and reused by future writes.
func (db *DB) Compact() error {
	// Snapshot the list of buckets so buckets created or deleted while
	// compacting do not affect the walk.
	var paths [][][]byte
	if err := db.View(func(tx *Tx) error {
		return tx.ForEach(func(name []byte, b *Bucket) error {
			return appendBucketPaths(b, [][]byte{cloneBytes(name)}, &paths)
		})
	}); err != nil {
		return err
	}

	for _, path := range paths {
		if err := db.compactBucket(path); err != nil {
			return err
		}
	}
	return nil
}

// appendBucketPaths appends the key path of the bucket and all of its
// nested buckets to paths.
func appendBucketPaths(b *Bucket, path [][]byte, paths *[][][]byte) error {
	*paths = append(*paths, path)
	return b.ForEach(func(k, v []byte) error {
		if v != nil {
			return nil
		}
		child := make([][]byte, len(path)+1)
		copy(child, path)
		child[len(path)] = cloneBytes(k)
		return appendBucketPaths(b.Bucket(k), child, paths)
	})
}

// compactBucket merges underfilled nodes in a single bucket, one chunk of
// leaf pages at a time.
func (db *DB) compactBucket(path [][]byte) error {
	threshold := int(float64(db.pageSize) * compactFillPercent)
	var seek []byte
	for {
		next, done, err := db.compactBucketChunk(path, seek, threshold)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		seek = next
	}
}

// compactBucketChunk examines a bounded number of leaf pages starting at the
// seek key and merges any that fit together with their siblings. It returns
// the key to resume from, or done if the bucket has been fully scanned.
func (db *DB) compactBucketChunk(path [][]byte, seek []byte, threshold int) (next []byte, done bool, err error) {
	err = db.Update(func(tx *Tx) error {
		b := openBucketPath(tx, path)
		if b == nil || b.root == 0 {
			// The bucket was deleted concurrently or is stored inline.
			done = true
			return nil
		}

		c := b.Cursor()
		var k []byte
		if seek == nil {
			k, _ = c.First()
		} else {
			k, _ = c.Seek(seek)
		}

		var work int
		for k != nil {
			// Skip leaves that are too full to absorb a sibling without
			// materializing a node.
			if c.leafSize() >= threshold {
				k, _ = c.Next()
				work++
			} else {
				// Fill the leaf from its right siblings as far as they
				// fit and compact any ancestors that became underfilled.
				n := c.node()
				for n.fillNext(threshold) {
					if work++; work >= compactMaxWorkPerTx {
						next = cloneBytes(n.inodes[0].key)
						return nil
					}
				}
				for p := n.parent; p != nil; p = p.parent {
					for p.fillNext(threshold) {
						if work++; work >= compactMaxWorkPerTx {
							next = cloneBytes(n.inodes[0].key)
							return nil
						}
					}
				}
				work++

				// Reposition the cursor past the (possibly grown) node.
				k, _ = c.Seek(n.inodes[len(n.inodes)-1].key)
				k, _ = c.Next()
			}

			if work >= compactMaxWorkPerTx {
				if k == nil {
					done = true
					return nil
				}
				next = cloneBytes(k)
				return nil
			}
		}
		done = true
		return nil
	})
	return next, done, err
}

// openBucketPath returns the bucket at the given key path or nil if it no
// longer exists.
func openBucketPath(tx *Tx, path [][]byte) *Bucket {
	b := tx.Bucket(path[0])
	for _, name := range path[1:] {
		if b == nil {
			return nil
		}
		b = b.Bucket(name)
	}
	return b
}

// leafSize returns the serialized size of the leaf page or node that the
// cursor is currently positioned on.
func (c *Cursor) leafSize() int {
	ref := c.stack[len(c.stack)-1]
	if ref.node != nil {
		return ref.node.size()
	}
	sz := pageHeaderSize
	p := ref.page
	for i := 0; i < int(p.count); i++ {
		elem := p.leafPageElement(uint16(i))
		sz += leafPageElementSize + int(elem.ksize) + int(elem.vsize)
	}
	return sz
}
