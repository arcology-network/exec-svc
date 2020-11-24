package types

import (
	"github.com/HPISTechnologies/common-lib/types"
	mevmCommon "github.com/HPISTechnologies/mevm/geth/common"
)

type ExecutorParameter struct {
	ParentInfo *types.ParentInfo
	Coinbase   *mevmCommon.Address
	Height     uint64
}
