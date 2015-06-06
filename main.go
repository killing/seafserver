package main

import (
	"flag"
	"github.com/killing/seafserver/ccnet"
	"github.com/killing/seafserver/log"
	"github.com/killing/searpc"
	"os"
	"path/filepath"
	"strconv"
)

type SeafRPCService int

func (svc *SeafRPCService) Echo(msg string) *searpc.Result {
	return &searpc.Result{Ret: msg}
}

type StringStruct struct {
	Msg string `json:"msg,omitempty"`
}

func (svc *SeafRPCService) List(msg string) *searpc.Result {
	var list []StringStruct = make([]StringStruct, 3)
	for i := 0; i < 3; i++ {
		list[i] = StringStruct{msg + strconv.Itoa(i)}
	}
	return &searpc.Result{Ret: list}
}

func main() {
	var ccnetConfDir, seafConfDir, logPath string
	var debug bool
	var logf *os.File
	var err error

	flag.StringVar(&ccnetConfDir, "c", "", "ccnet configuration directory path")
	flag.StringVar(&seafConfDir, "d", "", "seafile configuration directory path")
	flag.StringVar(&logPath, "l", "", "log file path")
	flag.BoolVar(&debug, "D", false, "enable debug logs")

	flag.Parse()

	if ccnetConfDir == "" || seafConfDir == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	ccnetConfDir, _ = filepath.Abs(ccnetConfDir)
	seafConfDir, _ = filepath.Abs(seafConfDir)

	if logPath == "" {
		logPath = filepath.Join(seafConfDir, "seafile.log")
	} else if logPath != "-" {
		logPath, _ = filepath.Abs(logPath)
	}

	if logPath != "-" {
		logf, err = os.OpenFile(logPath, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0666)
	} else {
		logf = os.Stderr
	}

	log.Init(logf, debug)

	rpcSvr := searpc.NewServer()
	svc := new(SeafRPCService)
	rpcSvr.Register(svc, "seafserv-rpcserver")

	client, err := ccnet.NewClient(ccnetConfDir, rpcSvr)
	if err != nil {
		log.Errorln("Failed to create ccnet client", err.Error())
		os.Exit(1)
	}

	err = client.RegisterProc("seafserv-rpcserver")
	if err != nil {
		log.Errorln("Failed to register rpc processor:", err.Error())
		os.Exit(1)
	}

	if err = client.Run(); err != nil {
		log.Errorln(err.Error())
	}

	return
}
