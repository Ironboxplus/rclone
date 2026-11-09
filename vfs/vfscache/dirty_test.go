package vfscache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dirtyChunk = 64 * 1024

// newDirtyTestCache makes a cache with asynchronous writeback whose
// uploads to the (local) remote are limited to bw bytes per second by the
// accounting token bucket, as with --bwlimit.
func newDirtyTestCache(t *testing.T, maxDirty, bw fs.SizeSuffix) (r *fstest.Run, c *Cache) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.HandleCaching = 0
	opt.WriteBack = fs.Duration(10 * time.Millisecond)
	opt.CacheMaxDirty = maxDirty
	r, c = newTestCacheOpt(t, opt)
	accounting.TokenBucket.SetBwLimit(fs.BwPair{Tx: bw, Rx: bw})
	t.Cleanup(func() { accounting.TokenBucket.SetBwLimit(fs.BwPair{Tx: -1, Rx: -1}) })
	return r, c
}

// dirtyContent returns the contents the tests write at off
func dirtyContent(off, size int64) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte((off + int64(i)) / 1021)
	}
	return b
}

// writeItem writes size bytes from off in chunks
func writeItem(item *Item, off, size int64) error {
	for end := off + size; off < end; off += dirtyChunk {
		n := min(dirtyChunk, end-off)
		if _, err := item.WriteAt(dirtyContent(off, n), off); err != nil {
			return err
		}
	}
	return nil
}

// writeFile writes and closes a new file
func writeFile(c *Cache, name string, size int64) error {
	item := c.Item(name)
	if err := item.Open(nil); err != nil {
		return err
	}
	err := writeItem(item, 0, size)
	if cerr := item.Close(nil); err == nil {
		err = cerr
	}
	return err
}

func checkUploaded(t *testing.T, r *fstest.Run, name string, size int64) {
	obj, err := r.Fremote.NewObject(context.Background(), name)
	require.NoError(t, err, name)
	in, err := obj.Open(context.Background())
	require.NoError(t, err)
	got, err := io.ReadAll(in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	assert.True(t, bytes.Equal(dirtyContent(0, size), got), "%s: wrong contents", name)
}

// maxDirtySeen samples c.DirtyBytes until done is closed
func maxDirtySeen(c *Cache, done <-chan struct{}) (maxSeen int64) {
	for {
		maxSeen = max(maxSeen, c.DirtyBytes())
		select {
		case <-done:
			return maxSeen
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func writeFiles(c *Cache, n int, size int64) (maxSeen int64, elapsed time.Duration, err error) {
	done := make(chan struct{})
	sampled := make(chan int64)
	go func() { sampled <- maxDirtySeen(c, done) }()
	start := time.Now()
	for i := range n {
		if err = writeFile(c, "file"+string(rune('a'+i)), size); err != nil {
			break
		}
	}
	elapsed = time.Since(start)
	close(done)
	return <-sampled, elapsed, err
}

func waitNoDirty(t *testing.T, c *Cache) {
	require.Eventually(t, func() bool { return c.DirtyBytes() == 0 }, 60*time.Second, 10*time.Millisecond)
}

// Writes wait for the uploads so the dirty bytes stay under the limit,
// and all the files arrive.
func TestDirtyWritesWaitForUploads(t *testing.T) {
	const size = 1 << 20
	r, c := newDirtyTestCache(t, 3*size, size) // uploads at 1 MiB/s
	maxSeen, elapsed, err := writeFiles(c, 5, size)
	require.NoError(t, err)
	assert.LessOrEqual(t, maxSeen, int64(3*size))
	// 5 MiB can't be written under a 3 MiB limit before 2 MiB have been uploaded
	assert.GreaterOrEqual(t, elapsed, time.Second)
	waitNoDirty(t, c)
	for i := range 5 {
		checkUploaded(t, r, "file"+string(rune('a'+i)), size)
	}
}

// Without the option the same writes go over the limit, which shows the
// test above sees the limit working.
func TestDirtyNoLimitByDefault(t *testing.T) {
	const size = 1 << 20
	r, c := newDirtyTestCache(t, 0, size)
	maxSeen, _, err := writeFiles(c, 5, size)
	require.NoError(t, err)
	assert.Greater(t, maxSeen, int64(3*size))
	waitNoDirty(t, c)
	for i := range 5 {
		checkUploaded(t, r, "file"+string(rune('a'+i)), size)
	}
}

// A file bigger than the limit can't be kept until it is uploaded, so
// the write which makes it too big fails at once with ENOSPC.
func TestDirtySingleFileOverLimit(t *testing.T) {
	const limit = 1 << 20
	r, c := newDirtyTestCache(t, limit, 16<<20)
	item := c.Item("big")
	require.NoError(t, item.Open(nil))
	err := writeItem(item, 0, 2*limit)
	require.Error(t, err)
	assert.True(t, errors.Is(err, syscall.ENOSPC), "%v", err)
	assert.LessOrEqual(t, c.DirtyBytes(), int64(limit))
	require.NoError(t, item.Close(nil))
	waitNoDirty(t, c)

	// the cache is still usable
	require.NoError(t, writeFile(c, "small", limit/2))
	waitNoDirty(t, c)
	checkUploaded(t, r, "small", limit/2)
}

// When only open files hold the dirty bytes no upload can lower them, so
// writing must not wait.
func TestDirtyOpenItemsDoNotDeadlock(t *testing.T) {
	const limit = 2 << 20
	_, c := newDirtyTestCache(t, limit, 16<<20)
	a, b := c.Item("a"), c.Item("b")
	require.NoError(t, a.Open(nil))
	require.NoError(t, b.Open(nil))
	done := make(chan error)
	go func() {
		err := writeItem(a, 0, limit-limit/4)
		if err == nil {
			err = writeItem(b, 0, limit/2)
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("write waited though only open files were dirty")
	}
	assert.Greater(t, c.DirtyBytes(), int64(limit))
	require.NoError(t, a.Close(nil))
	require.NoError(t, b.Close(nil))
	waitNoDirty(t, c)
}

// A write which waits goes on once the closed file ahead of it is uploaded.
func TestDirtyWriteResumesAfterUpload(t *testing.T) {
	const limit = 2 << 20
	r, c := newDirtyTestCache(t, limit, 512<<10) // 1.5 MiB takes 3 s
	require.NoError(t, writeFile(c, "first", limit-limit/4))

	done := make(chan error)
	go func() { done <- writeFile(c, "second", limit/2) }()
	select {
	case err := <-done:
		t.Fatalf("write did not wait for the upload: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	// "second" went on up to the limit and is waiting for "first" to be uploaded
	assert.Equal(t, int64(limit), c.DirtyBytes())

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("write did not go on after the upload")
	}
	checkUploaded(t, r, "first", limit-limit/4)
	waitNoDirty(t, c)
	checkUploaded(t, r, "second", limit/2)
}
