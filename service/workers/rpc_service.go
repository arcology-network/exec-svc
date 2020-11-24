package workers

import (
	"context"
	"errors"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	kafkalib "github.com/HPISTechnologies/component-lib/kafka/lib"
	"github.com/HPISTechnologies/component-lib/log"
	"go.uber.org/zap"
)

type ExecutorResponse struct {
	Responses []*types.ExecuteResponse
	Uuid      uint64
	SerialID  int
	Total     int
}

type RpcService struct {
	actor.WorkerThread
	wbs       *kafkalib.Waitobjs
	msgid     int64
	responses map[uint64][]*ExecutorResponse
}

//return a Subscriber struct
func NewRpcService(concurrency int, groupid string) *RpcService {

	rs := RpcService{}
	rs.Set(concurrency, groupid)
	rs.msgid = 0
	rs.responses = map[uint64][]*ExecutorResponse{}
	return &rs
}

func (rs *RpcService) OnStart() {
	rs.wbs = kafkalib.StartWaitObjects()
}

func (rs *RpcService) Stop() {

}

func (rs *RpcService) OnMessageArrived(msgs []*actor.Message) error {

	for _, v := range msgs {
		switch v.Name {
		case actor.MsgStartSub:
			rs.AddLog(log.LogLevel_Debug, "*************************************************************************")
		case actor.MsgTxsExecuteResults:
			exeResults := v.Data.(*ExecutorResponse)
			if exeResults != nil {
				list, ok := rs.responses[exeResults.Uuid]
				if !ok {
					list = make([]*ExecutorResponse, exeResults.Total)
					rs.responses[exeResults.Uuid] = list
				}
				list[exeResults.SerialID] = exeResults
				rs.AddLog(log.LogLevel_Debug, "received exec results*****subpack******", zap.Int("nums", len(exeResults.Responses)))
				results := make([]*types.ExecuteResponse, 0, exeResults.Total)
				for _, v := range list {
					if v == nil {
						return nil
					}
					results = append(results, v.Responses...)
				}

				delete(rs.responses, exeResults.Uuid)

				rs.AddLog(log.LogLevel_Debug, "received exec results***********", zap.Int64("msgid", rs.msgid))
				rs.wbs.Update(rs.msgid, &results)
			}

		}
	}

	return nil
}

func (rs *RpcService) ExecTxs(ctx context.Context, request *actor.Message, response *types.ExecutorResponses) error {

	lstMessage := request.CopyHeader()
	rs.ChangeEnvironment(lstMessage)

	rs.msgid = rs.msgid + 1

	rs.AddLog(log.LogLevel_Debug, "start exec request***********", zap.Int64("msgid", rs.msgid))

	rs.wbs.AddWaiter(rs.msgid)

	args := request.Data.(*types.ExecutorRequest)

	rs.MsgBroker.Send(actor.MsgTxs, args)

	rs.wbs.Waitforever(rs.msgid)
	results := rs.wbs.GetData(rs.msgid)

	var txResults *[]*types.ExecuteResponse

	if results == nil {
		rs.AddLog(log.LogLevel_Error, "data  error")
		return errors.New("data error")
	}

	if bValue, ok := results.(*[]*types.ExecuteResponse); ok {
		txResults = bValue
	} else {
		rs.AddLog(log.LogLevel_Error, "data type not mismatch error")
		return errors.New("data type not mismatch error")
	}

	rs.AddLog(log.LogLevel_Debug, "Exec return results***********", zap.Int("txResults", len(*txResults)))

	if txResults == nil {
		return nil
	}
	resultLength := len(*txResults)
	DfCalls := make([]*types.DeferCall, resultLength)
	HashList := make([]ethCommon.Hash, resultLength)
	StatusList := make([]uint64, resultLength)
	GasUsedList := make([]uint64, resultLength)
	for i, v := range *txResults {
		DfCalls[i] = v.DfCall
		HashList[i] = v.Hash
		StatusList[i] = v.Status
		GasUsedList[i] = v.GasUsed
	}
	response.DfCalls = DfCalls
	response.HashList = HashList
	response.StatusList = StatusList
	response.GasUsedList = GasUsedList
	//response.Results = *txResults
	return nil
}
