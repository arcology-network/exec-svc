package types

import (
	"github.com/HPISTechnologies/mevm/geth/common"
	"github.com/HPISTechnologies/mevm/geth/core/types"
)

type ExecutableTransaction struct {
	SnapshotHandle *SnapShot
	Messager       *types.Messager
	Revertings     *[]*common.Hash
}
