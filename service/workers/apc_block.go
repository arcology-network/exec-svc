package workers

import (
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/storage"
	execTypes "github.com/HPISTechnologies/exec-svc/service/types"
)

type ApcBlock struct {
	actor.WorkerThread
}

//return a Subscriber struct
func NewApcBlock(concurrency int, groupid string) *ApcBlock {

	apcBlock := ApcBlock{}
	apcBlock.Set(concurrency, groupid)

	return &apcBlock
}

func (apc *ApcBlock) OnStart() {
}

func (apc *ApcBlock) Stop() {

}

func (apc *ApcBlock) OnMessageArrived(msgs []*actor.Message) error {

	apcBlock := execTypes.ApcBlock{}
	for _, v := range msgs {
		switch v.Name {

		case actor.MsgApcHandle:
			apcBlock.ApcHandle = v.Data.(*storage.ApcHandle)
		case actor.MsgBlockCompleted:
			apcBlock.Result = v.Data.(string)
			apcBlock.ApcHeight = v.Height
			if apcBlock.Result == actor.MsgBlockCompleted_Success {
				apcBlock.ApcHeight = apcBlock.ApcHeight + 1
			}
		}
	}

	apc.MsgBroker.Send(actor.MsgApcBlock, &apcBlock)

	return nil
}
