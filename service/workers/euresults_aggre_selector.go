package workers

import (
	"errors"
	"time"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/aggregator/aggregator"
	"github.com/HPISTechnologies/component-lib/log"
	"go.uber.org/zap"
)

type EuResultsAggreSelector struct {
	actor.WorkerThread

	aggregator *aggregator.Aggregator
	msgType    string

	startTime time.Time
}

//return a Subscriber struct
func NewEuResultsAggreSelector(concurrency int, groupid string) *EuResultsAggreSelector {
	agg := EuResultsAggreSelector{}
	agg.Set(concurrency, groupid)

	agg.aggregator = aggregator.NewAggregator()

	return &agg
}

func (a *EuResultsAggreSelector) OnStart() {
}

func (a *EuResultsAggreSelector) OnMessageArrived(msgs []*actor.Message) error {

	if len(msgs) != 1 {
		a.AddLog(log.LogLevel_Info, "received params err", zap.Int("excepted", 1), zap.Int("received", len(msgs)))
		return errors.New("received params err")
	}

	switch msgs[0].Name {
	case actor.MsgBlockCompleted:
		remainingQuantity := a.aggregator.OnClearInfoReceived()
		a.AddLog(log.LogLevel_Info, "clear pool", zap.Int("remainingQuantity", remainingQuantity))
	case actor.MsgInclusive:
		inclusive := msgs[0].Data.(*types.InclusiveList)
		inclusive.Mode = types.InclusiveMode_Results
		a.startTime = time.Now()
		a.msgType = actor.MsgExecuted
		result, _ := a.aggregator.OnListReceived(inclusive)
		a.SendMsg(result)

	case actor.MsgPrecedingList:

		precedings := msgs[0].Data.(*[]*ethCommon.Hash)
		if precedings == nil {
			precedings = &[]*ethCommon.Hash{}
		}
		reapinglist := &types.ReapingList{
			List: *precedings,
		}

		a.msgType = actor.MsgPrecedingsEuresult
		a.startTime = time.Now()
		result, _ := a.aggregator.OnListReceived(reapinglist)
		a.SendMsg(result)
	case actor.MsgEuResults:
		data := msgs[0].Data.(*[]*types.EuResult)
		if data != nil && len(*data) > 0 {
			for _, v := range *data {
				euresult := v
				result := a.aggregator.OnDataReceived(ethCommon.BytesToHash([]byte(euresult.H)), euresult)
				a.SendMsg(result)
			}
		}
	}
	return nil
}
func (a *EuResultsAggreSelector) SendMsg(selectedData *[]*interface{}) {

	if selectedData != nil {
		euresults := make([]*types.EuResult, len(*selectedData))
		gatherTime := time.Now().Sub(a.startTime)
		startResult := time.Now()
		for i, euresult := range *selectedData {
			euresults[i] = (*euresult).(*types.EuResult)
		}
		a.AddLog(log.LogLevel_Info, "send gather result", zap.Int("counts", len(euresults)), zap.Duration("gather time", gatherTime), zap.Duration("result time", time.Now().Sub(startResult)))
		a.MsgBroker.Send(a.msgType, &euresults)
	}
}
