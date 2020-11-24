package types

import (
	"math/big"

	"github.com/HPISTechnologies/component-lib/storage"
	"github.com/HPISTechnologies/mevm/geth/common"
)

type TxToExecutes struct {
	Msgs          []*ExecMessager
	SnapShots     *storage.SnapShot
	PrecedingHash common.Hash
	Timestamp     *big.Int
}
