package vfscache

import (
	"context"
	"fmt"
	"sync"
	"syscall"
)

// dirtyAcct counts the bytes of the items which are not uploaded yet,
// so that writers can wait for uploads (--vfs-cache-max-dirty).
//
// Items report their share with Item._acct. mu is a leaf lock: it may
// be taken with Item.mu held, and nothing else is taken with it held.
type dirtyAcct struct {
	mu     sync.Mutex
	cond   sync.Cond
	bytes  int64 // size of all dirty items
	closed int64 // size of the dirty items which are closed, those are queued or uploading
}

func (d *dirtyAcct) init(ctx context.Context) {
	d.cond.L = &d.mu
	// release the writers waiting when the cache is shut down
	context.AfterFunc(ctx, func() {
		d.mu.Lock()
		d.cond.Broadcast()
		d.mu.Unlock()
	})
}

// add changes the counts, waking the waiting writers if they went down
func (d *dirtyAcct) add(bytes, closed int64) {
	d.mu.Lock()
	d.bytes += bytes
	d.closed += closed
	if bytes < 0 || closed < 0 {
		d.cond.Broadcast()
	}
	d.mu.Unlock()
}

// DirtyBytes returns the size of the items which are not uploaded yet
func (c *Cache) DirtyBytes() int64 {
	c.dirty.mu.Lock()
	defer c.dirty.mu.Unlock()
	return c.dirty.bytes
}

// waitDirty blocks while adding n more dirty bytes would go over
// --vfs-cache-max-dirty and some closed dirty item is queued or
// uploading, as the count goes down when that upload completes.
//
// When only open items hold the excess nothing can drain it, so it does
// not wait (no deadlock) and the quota of the cache disk is the limit.
//
// This is the backpressure of JuiceFS: writes wait while the buffer is
// over its size (pkg/vfs/writer.go:350-360), with a condition variable
// instead of sleeping. No error is returned for a full buffer, the
// writer is just slowed down to the speed of the uploads.
func (c *Cache) waitDirty(n int64) {
	limit := int64(c.opt.CacheMaxDirty)
	d := &c.dirty
	d.mu.Lock()
	defer d.mu.Unlock()
	for d.bytes+n > limit && d.closed > 0 && c.ctx.Err() == nil {
		d.cond.Wait()
	}
}

// _acct updates the share of the item in the dirty counts
//
// call with the lock held
func (item *Item) _acct() {
	var bytes, closed int64
	if item.info.Dirty {
		bytes = item.info.Size
		if item.opens == 0 {
			closed = bytes
		}
	}
	if bytes != item.acctBytes || closed != item.acctClosed {
		item.c.dirty.add(bytes-item.acctBytes, closed-item.acctClosed)
		item.acctBytes, item.acctClosed = bytes, closed
	}
}

// _dirtyGrowth is called with the lock held before a write of length
// bytes at off. It returns ENOSPC if the item alone would be over
// --vfs-cache-max-dirty (the whole file must stay local until it is
// uploaded, so a file bigger than that can't be written) and else the
// number of bytes which become dirty.
func (item *Item) _dirtyGrowth(off, length int64) (grow int64, err error) {
	size := item.info.Size
	newSize := max(size, off+length)
	if limit := int64(item.c.opt.CacheMaxDirty); newSize > limit {
		return 0, fmt.Errorf("vfs cache: %q would be %d bytes, over --vfs-cache-max-dirty %d: %w", item.name, newSize, limit, syscall.ENOSPC)
	}
	if item.info.Dirty {
		return newSize - size, nil
	}
	// the whole file becomes dirty
	return newSize, nil
}
