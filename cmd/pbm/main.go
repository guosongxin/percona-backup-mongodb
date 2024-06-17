package main

import (
	"github.com/percona/percona-backup-mongodb/cli"
)

// percona backup 命令行工具
// 连接 mongodb 集群 configsvr 分片，并操作对应的 collection 来达到提供命令行功能
// backup agent 会监听对应表更变然后镜像相应操作
func main() {
	cli.Main()
}
