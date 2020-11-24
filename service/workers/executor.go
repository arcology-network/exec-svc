package workers

import (
	"fmt"
	"math"
	"math/big"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	ethTypes "github.com/HPISTechnologies/3rd-party/eth/types"
	"github.com/HPISTechnologies/common-lib/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/log"
	"github.com/HPISTechnologies/component-lib/storage"
	"github.com/HPISTechnologies/concurrentlib/clib"
	execTypes "github.com/HPISTechnologies/exec-svc/service/types"
	mevmCommon "github.com/HPISTechnologies/mevm/geth/common"
	"github.com/HPISTechnologies/mevm/geth/core"
	adaptor "github.com/HPISTechnologies/vm-adaptor/evm"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type Executor struct {
	actor.WorkerThread

	serverID int64
	nthread  int64

	//chanTokenRing chan int

	euServices []execTypes.ExecutionService

	config *core.Config

	executorParameters *execTypes.ExecutorParameter

	msgsChan chan *execTypes.ExecMessagers
	exitChan chan bool

	txsCache map[uint64]*execTypes.TxToExecutes
}

//return a Subscriber struct
func NewExecutor(concurrency int, groupid string) *Executor {

	e := Executor{}
	e.Set(concurrency, groupid)

	e.serverID = viper.GetInt64("insid")

	if e.serverID > 255 || e.nthread > 255 {
		panic("At most 255 servers and 255 threads in each server are supported")
	}

	e.nthread = viper.GetInt64("nthread")

	e.euServices = make([]execTypes.ExecutionService, e.nthread)

	e.executorParameters = &execTypes.ExecutorParameter{}

	e.msgsChan = make(chan *execTypes.ExecMessagers, 50)
	e.exitChan = make(chan bool, 0)

	e.txsCache = map[uint64]*execTypes.TxToExecutes{}

	return &e
}

func (e *Executor) OnStart() {

	// TODO: create snapshot syncer
	snapshot := clib.NewSnapshot()
	//snapshot.Serve()

	apc := storage.NewAccountProxy(e.Concurrency)
	// TODO: create account cache

	var accCache core.EthAccountCache = &execTypes.ApcAdaptor{apc}
	// TODO: create ethereum storage cache
	var ethStorageCache core.EthStorageCache = apc

	// TODO: init config, update it on every new block height
	e.config = execTypes.MainConfig()

	for i := 0; i < int(e.nthread); i++ {
		euID := uint16(e.serverID<<8 | e.nthread)
		e.euServices[i].API = adaptor.NewAPI(euID, snapshot)
		state := core.NewStateDB(accCache, ethStorageCache, e.euServices[i].API)
		e.euServices[i].Eu = core.NewEU(uint16(e.serverID<<8|e.nthread), state, e.euServices[i].API, e.config)
	}

	e.startExec()
}

func (e *Executor) startExec() {
	for i := 0; i < int(e.nthread); i++ {
		index := i
		go func(index int) {
			for {
				select {
				case msgs := <-e.msgsChan:

					rst := execTypes.ExecutionRequest{
						Msgs: msgs.ExecMsgs,
					}

					e.euServices[index].Eu.SetApc(&execTypes.ApcCacheAdaptor{msgs.Snapshot.Apc}, msgs.Snapshot.Apc)
					e.euServices[index].API.SetSnapShot(*msgs.Snapshot.Snapshot)
					logid := e.AddLog(log.LogLevel_Debug, "exec request", zap.Int("mgs", len(rst.Msgs)), zap.String("coinbase", fmt.Sprintf("%x", msgs.Config.Coinbase)), zap.Int("index", index))
					interLog := e.GetLogger(logid)
					resp, err := e.euServices[index].Exec(&rst, *msgs.Config.Coinbase, interLog)
					if err != nil {
						e.AddLog(log.LogLevel_Error, "Exec err", zap.String("err", err.Error()))
						continue
					}

					e.sendResult(resp, msgs)

				case <-e.exitChan:
					break
				}
			}
		}(index)
	}
}

func (e *Executor) Stop() {

}
func (e *Executor) ExecMsgs(msgs *execTypes.TxToExecutes, height uint64) {

	if msgs == nil {
		return
	}

	e.config.Coinbase = e.executorParameters.Coinbase
	e.config.BlockNumber = new(big.Int).SetUint64(height)
	e.config.Time = msgs.Timestamp
	e.config.ParentHash = mevmCommon.BytesToHash(e.executorParameters.ParentInfo.ParentHash.Bytes())

	e.AddLog(log.LogLevel_Debug, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>start exec request", zap.Int("e.Msgs", len(msgs.Msgs)))

	groups := e.Groups(msgs.Msgs)

	for i := range groups {
		groups[i].Snapshot = msgs.SnapShots
		groups[i].Config = e.config
		//e.AddLog(log.LogLevel_Debug, "devide into group----------------->", zap.Int("groupidx", i), zap.Int("size", len(groups[i].ExecMsgs)))
		e.msgsChan <- groups[i]
	}

}
func (e *Executor) OnMessageArrived(msgs []*actor.Message) error {

	for _, v := range msgs {

		switch v.Name {
		case actor.MsgExecutorParameter:
			e.executorParameters = v.Data.(*execTypes.ExecutorParameter)
			if txs, ok := e.txsCache[e.executorParameters.Height]; ok {
				e.ExecMsgs(txs, e.executorParameters.Height)
				e.txsCache = map[uint64]*execTypes.TxToExecutes{}
			}
		case actor.MsgTxsToExecute:
			txs := msgs[0].Data.(*execTypes.TxToExecutes)
			if e.executorParameters.Height == v.Height {
				e.ExecMsgs(txs, v.Height)
			} else {
				e.txsCache[v.Height] = txs
			}

		}

	}

	return nil
}

func (e *Executor) Groups(msgs []*execTypes.ExecMessager) []*execTypes.ExecMessagers {

	groupSize := int(math.Min(float64(e.nthread), float64(len(msgs))))

	if groupSize == 0 {
		return []*execTypes.ExecMessagers{}
	}

	maxSize := len(msgs) / groupSize
	groups := make([]*execTypes.ExecMessagers, groupSize)
	uuid := common.GenerateUUID()

	e.AddLog(log.LogLevel_Debug, "Groups-------------", zap.Int("len(msgs)", len(msgs)), zap.Int("groupSize", groupSize), zap.Int("maxSize", maxSize))

	for i := range groups {

		endindex := (i + 1) * maxSize
		if i+1 == len(groups) {
			endindex = int(math.Max(float64((i+1)*maxSize), float64(len(msgs))))
		}
		messages := msgs[i*maxSize : endindex]

		groups[i] = &execTypes.ExecMessagers{
			Uuid:     uuid,
			Total:    groupSize,
			SerialID: i,
			ExecMsgs: messages,
		}
	}
	return groups
}

func (e *Executor) ToReceiptsHash(receipts *[]*ethTypes.Receipt) *types.ReceiptHashList {

	rcptLength := 0
	if receipts != nil {
		rcptLength = len(*receipts)
	}

	if rcptLength == 0 {
		return &types.ReceiptHashList{}
	}

	receiptHashList := make([]ethCommon.Hash, rcptLength)
	txHashList := make([]ethCommon.Hash, rcptLength)
	gasUsedList := make([]uint64, rcptLength)
	worker := func(start, end, idx int, args ...interface{}) {
		receipts := args[0].([]interface{})[0].([]*ethTypes.Receipt)
		receiptHashList := args[0].([]interface{})[1].([]ethCommon.Hash)
		txHashList := args[0].([]interface{})[2].([]ethCommon.Hash)
		gasUsedList := args[0].([]interface{})[3].([]uint64)

		for i := range receipts[start:end] {
			idx := i + start

			receipt := receipts[idx]
			txHashList[idx] = receipt.TxHash
			receiptHashList[idx] = ethCommon.RlpHash(*receipt)
			gasUsedList[idx] = receipt.GasUsed
		}
	}
	common.ParallelWorker(len(*receipts), e.Concurrency, worker, *receipts, receiptHashList, txHashList, gasUsedList)

	return &types.ReceiptHashList{
		TxHashList:      txHashList,
		ReceiptHashList: receiptHashList,
		GasUsedList:     gasUsedList,
	}
}

func (e *Executor) sendResult(response *execTypes.ExecutionResponse, messages *execTypes.ExecMessagers) {

	counter := len(response.EuResults)
	if counter > 0 {

		e.MsgBroker.Send(actor.MsgEuResults, &response.EuResults)
		txsResults := make([]*types.ExecuteResponse, counter)

		worker := func(start, end, idx int, args ...interface{}) {
			results := args[0].([]interface{})[0].([]*types.EuResult)
			responses := args[0].([]interface{})[1].([]*types.ExecuteResponse)

			for i := start; i < end; i++ {
				result := results[i]
				var df *types.DeferCall
				if result.DC != nil {
					df = &types.DeferCall{
						DeferID:         result.DC.DeferID,
						ContractAddress: types.Address(result.DC.ContractAddress),
						Signature:       result.DC.Signature,
					}
				}
				responses[i] = &types.ExecuteResponse{
					DfCall: df,
					//RevertedTxs: comlist,
					Hash:    ethCommon.BytesToHash([]byte(result.H)),
					Status:  result.Status,
					GasUsed: result.GasUsed,
				}
			}
		}
		common.ParallelWorker(len(response.EuResults), e.Concurrency, worker, response.EuResults, txsResults)

		responses := ExecutorResponse{
			Responses: txsResults,
			Uuid:      messages.Uuid,
			Total:     messages.Total,
			SerialID:  messages.SerialID,
		}

		e.MsgBroker.Send(actor.MsgTxsExecuteResults, &responses)

	}
	e.AddLog(log.LogLevel_Info, "send rws", zap.Int("nums", counter))
	counter = len(response.Receipts)
	if counter > 0 {

		e.MsgBroker.Send(actor.MsgReceipts, &response.Receipts)

		receiptHashList := e.ToReceiptsHash(&response.Receipts)
		e.MsgBroker.Send(actor.MsgReceiptHashList, receiptHashList)
	}
	e.AddLog(log.LogLevel_Info, "send receipt ", zap.Int("nums", counter))
}
