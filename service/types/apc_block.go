package types

import (
	"github.com/HPISTechnologies/component-lib/storage"
)

type ApcBlock struct {
	ApcHandle *storage.ApcHandle
	ApcHeight uint64
	Result    string
}
