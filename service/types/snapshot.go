package types

import (
	"github.com/HPISTechnologies/component-lib/storage"
	"github.com/HPISTechnologies/concurrentlib/clib"
)

type SnapShot struct {
	Snapshot *clib.Snapshot
	Apc      *storage.DirtyCache
}
