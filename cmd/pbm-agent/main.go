package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/alecthomas/kingpin"
	mlog "github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/agent"
	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/version"
)

const mongoConnFlag = "mongodb-uri"

// pbm-agent 入口点：
// 1. 接受命令行参数
// 2. 启动 pitr 协程，进行 oplog 流式备份
// 3. 启动心跳协程
// 4. 监听 pbmCmd collection，处理 pbm 发送的命令
func main() {
	var (
		pbmCmd = kingpin.New("pbm-agent", "Percona Backup for MongoDB")

		// run 子命令
		pbmAgentCmd = pbmCmd.Command("run", "Run agent").
				Default().
				Hidden()

		// run 子命令选项：-mongodb-uri 或环境变量 PBM_MONGODB_URI，用于获取 mongodb 集群连接端点
		mURI = pbmAgentCmd.Flag(mongoConnFlag, "MongoDB connection string").
			Envar("PBM_MONGODB_URI").
			Required().
			String()
		// run 子命令选项: -dump-parallel-collections 或环境变量 PBM_DUMP_PARALLEL_COLLECTIONS，配置 dump 操作的并行集合数量
		// TODO(study): 啥是 dump？对应到的什么形式的备份/恢复类型？
		dumpConns = pbmAgentCmd.Flag("dump-parallel-collections", "Number of collections to dump in parallel").
				Envar("PBM_DUMP_PARALLEL_COLLECTIONS").
				Default(strconv.Itoa(runtime.NumCPU() / 2)).
				Int()

		// run 子命令选项: version
		versionCmd   = pbmCmd.Command("version", "PBM version info")
		versionShort = versionCmd.Flag("short", "Only version info").
				Default("false").
				Bool()
		versionCommit = versionCmd.Flag("commit", "Only git commit info").
				Default("false").
				Bool()
		versionFormat = versionCmd.Flag("format", "Output format <json or \"\">").
				Default("").
				String()
	)

	cmd, err := pbmCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil && cmd != versionCmd.FullCommand() {
		log.Println("Error: Parse command line parameters:", err)
		return
	}

	if cmd == versionCmd.FullCommand() {
		switch {
		case *versionCommit:
			fmt.Println(version.Current().GitCommit)
		case *versionShort:
			fmt.Println(version.Current().Short())
		default:
			fmt.Println(version.Current().All(*versionFormat))
		}
		return
	}

	// hidecreds() will rewrite the flag content, so we have to make a copy before passing it on
	url := "mongodb://" + strings.Replace(*mURI, "mongodb://", "", 1)

	// 将 命令行参数里的 mongodb-uri 的 username 和 password 替换为 'x'，然后重新设置到 os.Args 中
	// TODO(study): why?
	hidecreds()

	err = runAgent(url, *dumpConns)
	log.Println("Exit:", err)
	if err != nil {
		os.Exit(1)
	}
}

func runAgent(mongoURI string, dumpConns int) error {
	mlog.SetDateFormat(plog.LogTimeFormat)
	mlog.SetVerbosity(&options.Verbosity{VLevel: mlog.DebugLow})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// pbmClient 持有一个 mongodb configsvr 的连接（不是 shard 模式下就是本节点）
	pbmClient, err := pbm.New(ctx, mongoURI, "pbm-agent")
	if err != nil {
		return errors.Wrap(err, "connect to PBM")
	}

	agnt := agent.New(pbmClient)
	defer agnt.Close()
	// agent 持有一个 mongodb 节点的连接
	err = agnt.AddNode(ctx, mongoURI, dumpConns)
	if err != nil {
		return errors.Wrap(err, "connect to the node")
	}
	agnt.InitLogger(pbmClient)

	if err := agnt.CanStart(); err != nil {
		return errors.WithMessage(err, "pre-start check")
	}

	// 启动 pitr oplog 流式备份
	go agnt.PITR()

	// 维持 agent 心跳，定时修改 pbmAgents 表，将此 agent 的各种状态写入其中，包括节点的类型，节点是否正常等信息
	go agnt.HbStatus()

	// 启动 agent
	return errors.Wrap(agnt.Start(), "listen the commands stream")
}
