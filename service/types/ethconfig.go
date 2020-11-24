package types

import (
	"math"
	"math/big"

	"github.com/HPISTechnologies/mevm/geth/common"
	"github.com/HPISTechnologies/mevm/geth/core"
	"github.com/HPISTechnologies/mevm/geth/core/types"
	"github.com/HPISTechnologies/mevm/geth/core/vm"
	"github.com/HPISTechnologies/mevm/geth/params"
)

// fakeChain implements the ChainContext interface.
type fakeChain struct {
}

func (chain *fakeChain) GetHeader(common.Hash, uint64) *types.Header {
	return &types.Header{}
}

//var coinbase = common.BytesToAddress([]byte{100, 100, 100})
var coinbase = common.HexToAddress("0x3d361736e7c94ee64f74c57a82b2af7ee17c2bf1")

func MainConfig() *core.Config {
	vmConfig := vm.Config{}
	cfg := &core.Config{
		ChainConfig: params.MainnetChainConfig,
		VMConfig:    &vmConfig,
		BlockNumber: new(big.Int).SetUint64(10000000),
		ParentHash:  common.Hash{},
		Time:        new(big.Int).SetUint64(10000000),
		Coinbase:    &coinbase,
		GasLimit:    math.MaxUint64,
		Difficulty:  new(big.Int).SetUint64(10000000),
	}
	cfg.Chain = new(fakeChain)
	return cfg

}
