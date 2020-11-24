package service

import (
	"net/http"

	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/rpc"
	"github.com/HPISTechnologies/component-lib/storage"
	"github.com/HPISTechnologies/component-lib/streamer"
	"github.com/HPISTechnologies/exec-svc/service/workers"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"

	"github.com/HPISTechnologies/component-lib/kafka"
)

type Config struct {
	concurrency int
	groupid     string
}

//return a Subscriber struct
func NewConfig() *Config {
	return &Config{
		concurrency: viper.GetInt("concurrency"),
		groupid:     "exec" + viper.GetString("insid"),
	}
}

func (cfg *Config) Start() {

	// logrus.SetLevel(logrus.DebugLevel)
	// logrus.SetFormatter(&logrus.JSONFormatter{})

	http.Handle("/streamer", promhttp.Handler())
	go http.ListenAndServe(":19005", nil)

	broker := streamer.NewStatefulStreamer()
	//00 initializer
	initializer := actor.NewActor(
		"initializer",
		broker,
		[]string{actor.MsgStarting},
		[]string{
			actor.MsgStartSub,
			actor.MsgBlockCompleted,
		},
		[]int{1, 1},
		workers.NewInitializer(cfg.concurrency, cfg.groupid),
	)
	initializer.Connect(streamer.NewDisjunctions(initializer, 1))

	//02 executor preparation
	executorPreparation := actor.NewActor(
		"executorPreparation",
		broker,
		[]string{
			actor.MsgCoinbase,
			actor.MsgParentInfo,
		},
		[]string{
			actor.MsgExecutorParameter,
		},
		[]int{1},
		workers.NewExecutorPreparation(cfg.concurrency, cfg.groupid),
	)
	executorPreparation.Connect(streamer.NewConjunctions(executorPreparation))

	//03 executor
	executor := actor.NewActor(
		"executor",
		broker,
		[]string{
			actor.MsgTxsToExecute,
			actor.MsgExecutorParameter,
		},
		[]string{
			actor.MsgReceipts,
			actor.MsgReceiptHashList,
			actor.MsgEuResults,
			actor.MsgTxsExecuteResults,
		},
		[]int{100, 100, 100, 1},
		workers.NewExecutor(cfg.concurrency, cfg.groupid),
	)
	executor.Connect(streamer.NewDisjunctions(executor, 200))

	receiveMseeages := []string{
		actor.MsgInitStatesUpdates,
		actor.MsgEuResults,
		actor.MsgInclusive,
		actor.MsgCoinbase,
		actor.MsgBlockCompleted,
		actor.MsgParentInfo,
	}

	receiveTopics := []string{
		viper.GetString("genesis-apc"),
		viper.GetString("scs-changes"),
		viper.GetString("inclusive-txs"),
		viper.GetString("msgexch"),
	}

	//04 apc module kafkaDownloader
	kafkaDownloader := actor.NewActor(
		"kafkaDownloader",
		broker,
		[]string{actor.MsgStartSub},
		receiveMseeages,
		[]int{1, 100, 1, 1, 1, 1},
		kafka.NewKafkaDownloader(cfg.concurrency, cfg.groupid, receiveTopics, receiveMseeages),
	)
	kafkaDownloader.Connect(streamer.NewDisjunctions(kafkaDownloader, 100))

	//05 aggre
	euresultAggreSelector := actor.NewActor(
		"euresultAggreSelector",
		broker,
		[]string{
			actor.MsgEuResults,
			actor.MsgInclusive,
			actor.MsgBlockCompleted,
			actor.MsgPrecedingList,
		},
		[]string{
			actor.MsgExecuted,
			actor.MsgPrecedingsEuresult,
		},
		[]int{1, 1},
		workers.NewEuResultsAggreSelector(cfg.concurrency, cfg.groupid),
	)
	euresultAggreSelector.Connect(streamer.NewDisjunctions(euresultAggreSelector, 1))

	//06 state applier
	stateApplier := actor.NewActor(
		"stateApplier",
		broker,
		[]string{
			actor.MsgExecuted,
			actor.MsgInclusive,
			actor.MsgApcHandle,
			actor.MsgCoinbase,
		},
		[]string{
			actor.MsgStatesUpdates,
		},
		[]int{1},
		storage.NewStateApplier(cfg.concurrency, cfg.groupid),
	)
	stateApplier.Connect(streamer.NewConjunctions(stateApplier))

	//07 apc
	apcWorker := actor.NewActor(
		"apcWorker",
		broker,
		[]string{
			actor.MsgStatesUpdates,
			actor.MsgBlockCompleted,
		},
		[]string{
			actor.MsgApcHandle,
		},
		[]int{1},
		storage.NewApcWorker(cfg.concurrency, cfg.groupid),
	)
	apcWorker.Connect(streamer.NewConjunctions(apcWorker))

	relations := map[string]string{}
	relations[actor.MsgReceipts] = viper.GetString("receipts-scs")
	relations[actor.MsgReceiptHashList] = viper.GetString("exec-rcpt-hash")
	relations[actor.MsgEuResults] = viper.GetString("scs-changes")
	//08 kafkaUploader
	kafkaUploader := actor.NewActor(
		"kafkaUploader",
		broker,
		[]string{
			actor.MsgReceipts,
			actor.MsgReceiptHashList,
			actor.MsgEuResults,
		},
		[]string{},
		[]int{},
		kafka.NewKafkaUploader(cfg.concurrency, cfg.groupid, relations),
	)
	kafkaUploader.Connect(streamer.NewDisjunctions(kafkaUploader, 4))

	//09 rpc-service
	rpcSvc := workers.NewRpcService(cfg.concurrency, cfg.groupid)

	rpcService := actor.NewActor(
		"rpcService",
		broker,
		[]string{
			actor.MsgStartSub,
			actor.MsgTxsExecuteResults,
		},
		[]string{actor.MsgTxs},
		[]int{1},
		rpcSvc,
	)
	rpcService.Connect(streamer.NewDisjunctions(rpcService, 1))

	//07 apcBlock
	apcBlock := actor.NewActor(
		"apcBlock",
		broker,
		[]string{
			actor.MsgApcHandle,
			actor.MsgBlockCompleted,
		},
		[]string{
			actor.MsgApcBlock,
		},
		[]int{1},
		workers.NewApcBlock(cfg.concurrency, cfg.groupid),
	)
	apcBlock.Connect(streamer.NewConjunctions(apcBlock))

	//10 snapshotMaker
	snapshotMaker := actor.NewActor(
		"snapshotMaker",
		broker,
		[]string{
			// actor.MsgBlockCompleted,
			// actor.MsgApcHandle,
			actor.MsgApcBlock,
			actor.MsgPrecedingsEuresult,
			actor.MsgTxs,
		},
		[]string{
			actor.MsgPrecedingList,
			actor.MsgTxsToExecute,
		},
		[]int{1, 1},
		workers.NewSnapshotMaker(cfg.concurrency, cfg.groupid),
	)
	snapshotMaker.Connect(streamer.NewDisjunctions(snapshotMaker, 2))

	//09 stateSwitcher
	stateSwitcher := actor.NewActor(
		"stateSwitcher",
		broker,
		[]string{
			actor.MsgInitStatesUpdates,
		},
		[]string{actor.MsgStatesUpdates},
		[]int{1},
		storage.NewStateSwitcher(cfg.concurrency, cfg.groupid),
	)
	stateSwitcher.Connect(streamer.NewDisjunctions(stateSwitcher, 1))

	//10 apchandleSwitcher
	apchandleSwitcher := actor.NewActor(
		"apchandleSwitcher",
		broker,
		[]string{
			actor.MsgApcHandle,
		},
		[]string{actor.MsgInitApcHandle},
		[]int{1},
		storage.NewApcHandleSwitcher(cfg.concurrency, cfg.groupid),
	)
	apchandleSwitcher.Connect(streamer.NewDisjunctions(apchandleSwitcher, 1))

	//starter
	selfStarter := streamer.NewDefaultProducer("selfStarter", []string{actor.MsgStarting}, []int{1})
	broker.RegisterProducer(selfStarter)

	broker.Serve()

	//start signel
	streamerStarting := actor.Message{
		Name:   actor.MsgStarting,
		Height: 0,
		Round:  0,
		Data:   "start",
	}
	broker.Send(actor.MsgStarting, &streamerStarting)

	rpcs := rpc.NewRpcServerPeer(viper.GetString("execAddr"))
	rpcs.RegisterSvc("executor", rpcSvc, "")
	rpcs.StartServer()
}

func (cfg *Config) Stop() {

}
