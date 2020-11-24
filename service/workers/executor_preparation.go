package workers

import (
	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	execTypes "github.com/HPISTechnologies/exec-svc/service/types"
	mevmCommon "github.com/HPISTechnologies/mevm/geth/common"
)

type ExecutorPreparation struct {
	actor.WorkerThread
}

//return a Subscriber struct
func NewExecutorPreparation(concurrency int, groupid string) *ExecutorPreparation {

	ep := ExecutorPreparation{}
	ep.Set(concurrency, groupid)

	return &ep
}

func (ep *ExecutorPreparation) OnStart() {
}

func (ep *ExecutorPreparation) Stop() {

}

func (ep *ExecutorPreparation) OnMessageArrived(msgs []*actor.Message) error {

	executorParameters := execTypes.ExecutorParameter{}

	for _, v := range msgs {
		switch v.Name {

		case actor.MsgCoinbase:
			coinbase := v.Data.(*ethCommon.Address)
			gcoinbase := mevmCommon.BytesToAddress(coinbase.Bytes())
			executorParameters.Coinbase = &gcoinbase
			executorParameters.Height = ep.LatestMessage.Height
		case actor.MsgParentInfo:
			parentinfo := v.Data.(*types.ParentInfo)
			executorParameters.ParentInfo = parentinfo
		}
	}

	ep.MsgBroker.Send(actor.MsgExecutorParameter, &executorParameters)

	return nil
}
