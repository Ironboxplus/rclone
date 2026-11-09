//go:build unix

// The serving is tested in cmd/nfsmount - here we test anything else
package nfs

import (
	"testing"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/cmd/serve/servetest"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRc(t *testing.T) {
	servetest.TestRc(t, rc.Params{
		"type":           "nfs",
		"vfs_cache_mode": "off",
	})
}

// serve/start reads the VFS options from the parameters like this
func TestRcVFSCacheMaxDirty(t *testing.T) {
	assert.Equal(t, fs.SizeSuffix(0), vfscommon.Opt.CacheMaxDirty, "off by default")
	opt := vfscommon.Opt
	require.NoError(t, rc.ParseOptions(rc.Params{"vfs_cache_max_dirty": "64M"}, "vfsOpt", &opt))
	assert.Equal(t, 64*fs.Mebi, opt.CacheMaxDirty)
}
