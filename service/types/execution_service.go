package types

import (
	"math/big"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	ethTypes "github.com/HPISTechnologies/3rd-party/eth/types"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/log"
	"github.com/HPISTechnologies/component-lib/storage"
	mevmCommon "github.com/HPISTechnologies/mevm/geth/common"
	"github.com/HPISTechnologies/mevm/geth/core"
	mevmTypes "github.com/HPISTechnologies/mevm/geth/core/types"
	adaptor "github.com/HPISTechnologies/vm-adaptor/evm"
	"go.uber.org/zap"
)

type ExecMessager struct {
	Msg *mevmTypes.Messager
}

type ExecMessagers struct {
	Snapshot *storage.SnapShot
	Config   *core.Config
	ExecMsgs []*ExecMessager
	Uuid     uint64
	SerialID int
	Total    int
}

func (em *ExecMessager) ParseMessage(msg *types.StandardMessage) {
	em.Msg.Txhash = mevmCommon.BytesToHash(msg.TxHash.Bytes())

	var message mevmTypes.Message
	if msg.Native.To() != nil {
		to := mevmCommon.BytesToAddress(msg.Native.To().Bytes())
		message = mevmTypes.NewMessage(
			mevmCommon.BytesToAddress(msg.Native.From().Bytes()),
			&to,
			msg.Native.Nonce(),
			msg.Native.Value(),
			msg.Native.Gas(),
			msg.Native.GasPrice(),
			msg.Native.Data(),
			msg.Native.CheckNonce(),
		)
	} else {
		message = mevmTypes.NewMessage(
			mevmCommon.BytesToAddress(msg.Native.From().Bytes()),
			nil,
			msg.Native.Nonce(),
			msg.Native.Value(),
			msg.Native.Gas(),
			msg.Native.GasPrice(),
			msg.Native.Data(),
			msg.Native.CheckNonce(),
		)
	}

	em.Msg.Msg = &message
}

type ExecutionRequest struct {
	Msgs []*ExecMessager
}

type ExecutionResponse struct {
	EuResults []*types.EuResult
	Receipts  []*ethTypes.Receipt
}

type ExecutionService struct {
	Eu  *core.EU
	API *adaptor.API
}

func ConvertReceipt(ereceipt *mevmTypes.Receipt) *ethTypes.Receipt {
	dreceipt := ethTypes.Receipt{}
	if ereceipt != nil {
		dreceipt.PostState = ereceipt.PostState
		dreceipt.Status = ereceipt.Status
		dreceipt.CumulativeGasUsed = ereceipt.CumulativeGasUsed
		dreceipt.Bloom = ethTypes.BytesToBloom(ereceipt.Bloom.Bytes())
		logs := make([]*ethTypes.Log, len(ereceipt.Logs))
		for i, log := range ereceipt.Logs {

			topics := make([]ethCommon.Hash, len(log.Topics))
			for j, topic := range log.Topics {
				topics[j] = ethCommon.BytesToHash(topic.Bytes())
			}
			logs[i] = &ethTypes.Log{
				Address:     ethCommon.BytesToAddress(log.Address.Bytes()),
				Topics:      topics,
				Data:        log.Data,
				BlockNumber: log.BlockNumber,
				TxHash:      ethCommon.BytesToHash(log.TxHash.Bytes()),
				TxIndex:     log.TxIndex,
				BlockHash:   ethCommon.BytesToHash(log.BlockHash.Bytes()),
				Index:       log.Index,
				Removed:     log.Removed,
			}
		}
		dreceipt.Logs = logs
		dreceipt.TxHash = ethCommon.BytesToHash(ereceipt.TxHash.Bytes())
		dreceipt.ContractAddress = ethCommon.BytesToAddress(ereceipt.ContractAddress.Bytes())
		dreceipt.GasUsed = ereceipt.GasUsed
	}
	return &dreceipt
}
func (es *ExecutionService) Exec(request *ExecutionRequest, coinbase mevmCommon.Address, logg *actor.WorkerThreadLogger) (*ExecutionResponse, error) {
	//response.EuResults
	response := ExecutionResponse{}
	msgLen := len(request.Msgs)
	response.EuResults = make([]*types.EuResult, msgLen)
	response.Receipts = make([]*ethTypes.Receipt, msgLen)
	logg.Log(log.LogLevel_Info, "service.Exec", zap.Int("nums", msgLen))
	failed := 0
	for i, m := range request.Msgs {
		th := mevmCommon.Hash(m.Msg.Txhash)

		// TODO: Get predecessor list from request.
		// TODO: Update snapshot based on predecessor list.
		es.API.SetPredecessors([]mevmCommon.Hash{})

		euResult, receipt := es.Eu.Run(th, m.Msg.Msg, coinbase)
		rs, ws := es.API.Collect()
		if euResult.Status == ethTypes.ReceiptStatusSuccessful { // success
			balanceReads := make(map[types.Address]*big.Int)
			for addr, amount := range euResult.R.BalanceReads {
				balanceReads[types.Address(addr.Bytes())] = amount
			}
			storageReads := make([]types.Address, 0, len(euResult.R.EthStorageReads))
			for _, addr := range euResult.R.EthStorageReads {
				storageReads = append(storageReads, types.Address(addr.Bytes()))
			}
			reads := &types.Reads{
				ClibReads:       rs,
				BalanceReads:    balanceReads,
				EthStorageReads: storageReads,
			}
			newAccounts := make([]types.Address, 0, len(euResult.W.NewAccounts))
			for _, addr := range euResult.W.NewAccounts {
				newAccounts = append(newAccounts, types.Address(addr.Bytes()))
			}
			balanceWrites := make(map[types.Address]*big.Int)
			for addr, amount := range euResult.W.BalanceWrites {
				balanceWrites[types.Address(addr.Bytes())] = amount
			}
			nonceWrites := make(map[types.Address]uint64)
			for addr, nonce := range euResult.W.NonceWrites {
				nonceWrites[types.Address(addr.Bytes())] = nonce
			}
			codeWrites := make(map[types.Address][]byte)
			for addr, code := range euResult.W.CodeWrites {
				codeWrites[types.Address(addr.Bytes())] = code
			}
			storageWrites := make(map[types.Address]map[string]string)
			for addr, ethStorageWrite := range euResult.W.EthStorageWrites {
				sw := make(map[string]string)
				for k, v := range ethStorageWrite {
					sw[string(k.Bytes())] = string(v.Bytes())
				}
				storageWrites[types.Address(addr.Bytes())] = sw
			}
			writes := &types.Writes{
				ClibWrites:       ws,
				NewAccounts:      newAccounts,
				BalanceWrites:    balanceWrites,
				NonceWrites:      nonceWrites,
				CodeWrites:       codeWrites,
				EthStorageWrites: storageWrites,
			}
			response.EuResults[i] = &types.EuResult{
				R: reads,
				W: writes,
			}
		} else { // fail
			balanceWrites := make(map[types.Address]*big.Int)
			for addr, amount := range euResult.W.BalanceWrites {
				balanceWrites[types.Address(addr.Bytes())] = amount
			}
			writes := &types.Writes{
				BalanceWrites: balanceWrites,
			}
			response.EuResults[i] = &types.EuResult{
				W: writes,
			}
			failed++
		}

		balanceOrigin := make(map[types.Address]*big.Int)
		for addr, amount := range euResult.W.BalanceOrigin {
			balanceOrigin[types.Address(addr.Bytes())] = amount
		}
		response.EuResults[i].W.BalanceOrigin = balanceOrigin

		response.EuResults[i].H = string(euResult.H.Bytes())
		response.EuResults[i].GasUsed = euResult.GasUsed
		response.EuResults[i].Status = euResult.Status

		response.EuResults[i].DC = es.API.GetDeferCall()

		response.Receipts[i] = ConvertReceipt(receipt)
	}
	logg.Log(log.LogLevel_Info, "service.Exec end", zap.Int("nums", msgLen), zap.Int("failed", failed))
	return &response, nil
}
