package main

import (
	"flag"
	"fmt"
	"github.com/killing/seafserver/ccnet"
	"github.com/killing/searpc"
	"os"
	"path/filepath"
)

type SeafRPCService int

func (svc *SeafRPCService) Echo(msg string) *searpc.Result {
	return &searpc.Result{Ret:msg}
}

func main() {
	var ccnetConfDir string

	flag.StringVar(&ccnetConfDir, "c", "", "ccnet configuration directory path")

	flag.Parse()

	if ccnetConfDir == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	ccnetConfDir, _ = filepath.Abs(ccnetConfDir)

	rpcSvr := searpc.NewServer()
	svc := new(SeafRPCService)
	rpcSvr.Register(svc, "seafserv-rpcserver")

	client, err := ccnet.NewClient(ccnetConfDir, rpcSvr)
	if err != nil {
		fmt.Println("Failed to create ccnet client", err.Error())
		os.Exit(1)
	}

	err = client.RegisterProc("seafserv-rpcserver")
	if err != nil {
		fmt.Println("Failed to register rpc processor:", err.Error())
		os.Exit(1)
	}

	if err = client.Run(); err != nil {
		fmt.Println(err.Error())
	}

	return
}
