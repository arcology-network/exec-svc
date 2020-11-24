package types

import (
	"github.com/HPISTechnologies/component-lib/storage"
	"github.com/HPISTechnologies/mevm/geth/core"
)

type ApcAdaptor struct {
	Apc *storage.AccountProxy
}

func (adaptor *ApcAdaptor) GetAccount(addr string) (core.Account, error) {
	return adaptor.Apc.GetAccount(addr)
}

func (adaptor *ApcAdaptor) GetCode(addr string) ([]byte, error) {
	return adaptor.Apc.GetCode(addr)
}

type ApcCacheAdaptor struct {
	Apc *storage.DirtyCache
}

func (adaptor *ApcCacheAdaptor) GetAccount(addr string) (core.Account, error) {
	return adaptor.Apc.GetAccount(addr)
}

func (adaptor *ApcCacheAdaptor) GetCode(addr string) ([]byte, error) {
	return adaptor.Apc.GetCode(addr)
}
