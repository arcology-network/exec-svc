package workers

import (
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/storage"
	"go.uber.org/zap"

	execTypes "github.com/HPISTechnologies/exec-svc/service/types"
	"github.com/HPISTechnologies/mevm/geth/common"

	"github.com/HPISTechnologies/component-lib/log"
	mevmTypes "github.com/HPISTechnologies/mevm/geth/core/types"
)

type SnapshotMaker struct {
	actor.WorkerThread

	snapshots map[common.Hash]*storage.SnapShot
	txs       execTypes.TxToExecutes

	apcBlock *execTypes.ApcBlock

	paramsCache map[uint64]*types.ExecutorRequest
}

//return a Subscriber struct
func NewSnapshotMaker(concurrency int, groupid string) *SnapshotMaker {

	sm := SnapshotMaker{}
	sm.Set(concurrency, groupid)

	return &sm
}

func (sm *SnapshotMaker) OnStart() {
	sm.snapshots = map[common.Hash]*storage.SnapShot{}
	sm.paramsCache = map[uint64]*types.ExecutorRequest{}

	sm.apcBlock = &execTypes.ApcBlock{
		ApcHeight: 0,
	}
}

func (sm *SnapshotMaker) Stop() {

}

func (sm *SnapshotMaker) createFirstSnapShot() {

	snapshotTransient := (*sm.apcBlock.ApcHandle.Snapshot).CreateTransientSnapshot()

	//dirthCache := storage.NewDirtyCache(&execTypes.ApcAdaptor{sm.apcBlock.ApcHandle.Apc}, sm.apcBlock.ApcHandle.Apc)

	dirthCache := storage.NewDirtyCache(sm.apcBlock.ApcHandle.Apc, sm.apcBlock.ApcHandle.Apc)

	snapshot := storage.SnapShot{
		Snapshot: &snapshotTransient,
		Apc:      dirthCache,
	}
	sm.snapshots[common.Hash{}] = &snapshot
}

func (sm *SnapshotMaker) execResult(results []*types.EuResult) {
	if results == nil {
		return
	}
	snapshot := storage.SnapShot{
		Snapshot: sm.apcBlock.ApcHandle.Snapshot,
		Apc:      storage.NewDirtyCache(sm.apcBlock.ApcHandle.Apc, sm.apcBlock.ApcHandle.Apc),
	}
	updates := storage.StatesUpdates{
		FinalEuResults: &results,
	}
	newsnapshot := storage.FlushApcSnapShot(&snapshot, &updates, false)

	sm.snapshots[sm.txs.PrecedingHash] = newsnapshot
	sm.txs.SnapShots = newsnapshot
	sm.MsgBroker.Send(actor.MsgTxsToExecute, &sm.txs)
}

func (sm *SnapshotMaker) OnMessageArrived(msgs []*actor.Message) error {

	for _, v := range msgs {

		switch v.Name {

		case actor.MsgApcBlock:
			sm.apcBlock = v.Data.(*execTypes.ApcBlock)
			sm.createFirstSnapShot()
			if params, ok := sm.paramsCache[sm.apcBlock.ApcHeight]; ok {
				sm.AddLog(log.LogLevel_Debug, "exec MsgApcHandle", zap.Uint64("apcHeight", sm.apcBlock.ApcHeight))
				sm.execRequest(params)
				sm.paramsCache = map[uint64]*types.ExecutorRequest{}
			}
		case actor.MsgPrecedingsEuresult:
			results := v.Data.(*[]*types.EuResult)
			if results != nil {
				sm.execResult(*results)
			}
		case actor.MsgTxs:
			params := v.Data.(*types.ExecutorRequest)
			if v.Height == sm.apcBlock.ApcHeight {
				sm.AddLog(log.LogLevel_Debug, "exec ExecutorRequest", zap.Uint64("apcHeight", sm.apcBlock.ApcHeight), zap.Uint64("ExecutorRequest height", v.Height))
				sm.execRequest(params)
			} else {
				sm.paramsCache[v.Height] = params
				sm.AddLog(log.LogLevel_Debug, "cache ExecutorRequest", zap.Uint64("apcHeight", sm.apcBlock.ApcHeight), zap.Uint64("ExecutorRequest height", v.Height))
			}
		}

	}

	return nil
}

func (sm *SnapshotMaker) execRequest(params *types.ExecutorRequest) {
	if params != nil {

		msgs := make([]*execTypes.ExecMessager, len(params.Msgs))

		for i, item := range params.Msgs {

			emsg := execTypes.ExecMessager{
				Msg: &mevmTypes.Messager{},
			}
			emsg.ParseMessage(item)

			msgs[i] = &emsg
		}

		sm.txs = execTypes.TxToExecutes{
			Msgs:      msgs,
			Timestamp: params.Timestamp,
		}

		snapshotHash := common.BytesToHash(params.PrecedingHash.Bytes())
		sm.txs.PrecedingHash = snapshotHash

		if snapshot, ok := sm.snapshots[snapshotHash]; ok {
			sm.txs.SnapShots = snapshot

			sm.MsgBroker.Send(actor.MsgTxsToExecute, &sm.txs)
		} else {

			sm.MsgBroker.Send(actor.MsgPrecedingList, &params.Precedings)
		}

	}
}
