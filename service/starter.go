package service

import (
	tmCommon "github.com/HPISTechnologies/3rd-party/tm/common"
	"github.com/HPISTechnologies/component-lib/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var StartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start actuator service Daemon",
	RunE:  startCmd,
}

func init() {
	flags := StartCmd.Flags()

	flags.String("mqaddr", "localhost:9092", "host:port of kafka ")

	flags.String("msgexch", "msgexch", "topic for receive msg exchange")
	flags.Int("concurrency", 4, "num of threads")
	flags.String("scs-changes", "scs-changes", "topic for received smart contract result")
	flags.String("exec-rcpt-hash", "exec-rcpt-hash", "topic for send  receipts hash")
	flags.String("receipts-scs", "receipts-scs", "topic for send  receipts of smartcontracts")
	flags.String("logcfg", "./log.toml", "log conf path")
	flags.Bool("draw", false, "draw flow graph")
	flags.String("inclusive-txs", "inclusive-txs", "topic of send txlist")
	flags.String("genesis-apc", "genesis-apc", "topic for received genesis apc")
	flags.Uint64("insid", 1, "instance id of exector,range 1 to 255")
	flags.Uint64("nthread", 4, "Num of thread,range 1 to 255")
	flags.String("execAddr", "localhost:8973", "executor server address")

	flags.Int("nidx", 0, "node index in cluster")
	flags.String("nname", "node1", "node name in cluster")
}

func startCmd(cmd *cobra.Command, args []string) error {

	log.InitLog("exec.log", viper.GetString("logcfg"), "exec", viper.GetString("nname"), viper.GetInt("nidx"))

	en := NewConfig()
	en.Start()

	if viper.GetBool("draw") {
		log.CompleteMetaInfo("exec")
		return nil
	}

	// Wait forever
	tmCommon.TrapSignal(func() {
		// Cleanup
		en.Stop()
	})

	return nil
}
