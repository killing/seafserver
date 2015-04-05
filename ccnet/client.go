// Package ccnet implements basic client library to ccnet server.

package ccnet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/killing/searpc"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	unixSockName string = "ccnet.sock"
)

const (
	StatusCodeProcDead   string = "102"
	StatusStringProcDead string = "Processor Is Dead"

	StatusCodeOK   string = "200"
	StatusStringOK string = "OK"

	StatusCodeCall   string = "301"
	StatusStringCall string = "Client Call"

	StatusCodeRet   string = "311"
	StatusStringRet string = "Server Ret"

	StatusCodeCreateProcErr   string = "501"
	StatusStringCreateProcErr string = "Create Processor Error"

	StatusCodeBadArgs   string = "503"
	StatusStringBadArgs string = "Bad Arguments"

	StatusCodeBadUpdate   string = "506"
	StatusStringBadUpdate string = "Bad Update Code"
)

const (
	protocolVersion uint8 = 1

	MsgTypeRequest  uint8 = 2
	MsgTypeResponse uint8 = 3
	MsgTypeUpdate   uint8 = 4
)

var (
	BadProtocolVersion = errors.New("ccnet: bad protocol version")
	BadMessageFormat   = errors.New("ccnet: bad message format")
)

// Message is sent and recieved by the client
type Message struct {
	MsgType uint8
	Code    string
	CodeMsg string
	Content []byte
}

func (msg *Message) len() uint16 {
	var ret int

	switch msg.MsgType {
	case MsgTypeRequest:
		return uint16(len(msg.Content))
	case MsgTypeUpdate, MsgTypeResponse:
		ret = len(msg.Code)
		if msg.CodeMsg != "" {
			ret += (1 + len(msg.CodeMsg))
		}
		ret += (1 + len(msg.Content))
		return uint16(ret)
	default:
		return 0
	}
}

// Client manages a connection to the ccnet server
type Client struct {
	conn         net.Conn          // connection to ccnet server
	wl           sync.Mutex        // synchronize writes to the connection
	rw           *bufio.ReadWriter // buffer wrapped around the connection.
	procRegistry map[string]int    // Record whether a processor name is registered.
	procMap      map[int]string    // request id to processor (service) name
	rpcServer    *searpc.Server
	reqId        int32 // auto-increment request id.
}

func NewClient(confDir string, rpcServer *searpc.Server) (client *Client, err error) {
	var conn net.Conn
	unixSockPath := confDir + "/" + unixSockName
	if conn, err = net.Dial("unix", unixSockPath); err != nil {
		log.Println(err.Error())
		return
	}

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	client = &Client{
		conn:         conn,
		rw:           rw,
		procRegistry: make(map[string]int),
		rpcServer:    rpcServer,
		procMap:      make(map[int]string),
	}

	return
}

// RegisterProc registers an RPC service name.
// It should be called before calling Run()
func (client *Client) RegisterProc(procName string) error {
	var err error
	var msg *Message
	var errMsg string

	if _, present := client.procRegistry[procName]; present {
		return nil
	}

	reqId := client.NextReqId()

	msg = &Message{MsgType: MsgTypeRequest, Content: []byte("receive-cmd")}
	if err = client.SendMessage(reqId, msg); err != nil {
		return err
	}

	if _, msg, err = client.RecvMessage(); err != nil {
		return err
	}
	if msg.MsgType != MsgTypeResponse || msg.Code != StatusCodeOK {
		log.Println(msg.MsgType, msg.Code)
		errMsg = "ccnet: Received bad response for request"
		return errors.New(errMsg)
	}

	upd := "register-service " + procName + " rpc-inner"
	buf := make([]byte, len(upd)+1)
	copy(buf, upd)
	msg = &Message{
		MsgType: MsgTypeUpdate,
		Code:    StatusCodeOK,
		CodeMsg: StatusStringOK,
		Content: buf,
	}
	if err = client.SendMessage(reqId, msg); err != nil {
		return err
	}

	if _, msg, err = client.RecvMessage(); err != nil {
		return err
	}
	if msg.MsgType != MsgTypeResponse || msg.Code != StatusCodeOK {
		errMsg = "ccnet: Received bad response for update"
		return errors.New(errMsg)
	}

	client.procRegistry[procName] = 1

	return nil
}

func (client *Client) NextReqId() (reqId int32) {
	reqId = atomic.AddInt32(&client.reqId, 1)
	return
}

func (client *Client) writeHeader(msgType uint8, reqId int32, contentLen uint16) error {
	var err error

	log.Println("Writing message header:", reqId, msgType, contentLen)

	if err = client.rw.WriteByte(byte(protocolVersion)); err != nil {
		return err
	}

	if err = client.rw.WriteByte(byte(msgType)); err != nil {
		return err
	}

	if err = binary.Write(client.rw, binary.BigEndian, contentLen); err != nil {
		return err
	}

	if err = binary.Write(client.rw, binary.BigEndian, reqId); err != nil {
		return err
	}

	return nil
}

func (client *Client) writeMessage(msg *Message) error {
	var err error

	log.Println("Writing message body:", string(msg.Content))

	switch msg.MsgType {
	case MsgTypeRequest:
		_, err = client.rw.Write(msg.Content)
		return err
	case MsgTypeUpdate, MsgTypeResponse:
		if _, err = client.rw.WriteString(msg.Code); err != nil {
			return err
		}

		if msg.CodeMsg != "" {
			if err = client.rw.WriteByte(' '); err != nil {
				return err
			}
			if _, err = client.rw.WriteString(msg.CodeMsg); err != nil {
				return err
			}
		}

		if err = client.rw.WriteByte('\n'); err != nil {
			return err
		}

		if msg.Content != nil {
			_, err = client.rw.Write(msg.Content)
		}

		return err
	default:
		log.Panic("ccnet: invalid message type.")
	}

	return nil
}

func (client *Client) SendMessage(reqId int32, msg *Message) error {
	var err error

	client.wl.Lock()
	defer client.wl.Unlock()

	if err = client.writeHeader(msg.MsgType, reqId, msg.len()); err != nil {
		return err
	}

	if err = client.writeMessage(msg); err != nil {
		return err
	}

	err = client.rw.Flush()

	return err
}

func (client *Client) RecvMessage() (reqId int32, msg *Message, err error) {
	var msgType uint8
	var msgLen uint16

	if msgType, reqId, msgLen, err = client.readHeader(); err != nil {
		return
	}

	msg, err = client.readMessage(msgType, int(msgLen))

	return
}

func (client *Client) readHeader() (msgType uint8, reqId int32, msgLen uint16, err error) {
	var version uint8

	log.Println("Reading message header")

	if version, err = client.rw.ReadByte(); err != nil {
		return
	}
	if version != protocolVersion {
		err = BadProtocolVersion
		return
	}

	log.Println("Message version", version)

	if msgType, err = client.rw.ReadByte(); err != nil {
		return
	}

	log.Println("Message type", msgType)

	if err = binary.Read(client.rw, binary.BigEndian, &msgLen); err != nil {
		return
	}

	log.Println("Message length", msgLen)

	if err = binary.Read(client.rw, binary.BigEndian, &reqId); err != nil {
		return
	}

	log.Println("Request ID", reqId)

	return
}

func (client *Client) readMessage(msgType uint8, msgLen int) (msg *Message, err error) {
	var buf = make([]byte, msgLen)
	var code, codeMsg string
	var content []byte

	if _, err = io.ReadFull(client.rw, buf); err != nil {
		return
	}

	log.Println("Read message body:", string(buf))

	switch msgType {
	case MsgTypeRequest:
		msg = &Message{MsgType: msgType, Content: buf}
	case MsgTypeUpdate, MsgTypeResponse:
		if msgLen < 4 {
			err = BadMessageFormat
			return
		}

		code = string(buf[:3])
		buf = buf[3:]
		msgLen -= 3

		index := bytes.IndexByte(buf, '\n')
		if index == -1 {
			err = BadMessageFormat
			return
		}
		if buf[0] == ' ' {
			codeMsg = string(buf[:index])
		}

		if msgLen > index+1 {
			content = make([]byte, msgLen-index-1)
			copy(content, buf[index+1:])
		}
		msg = &Message{msgType, code, codeMsg, content}
	}
	return
}

// Run handles incoming RPC service calls.
func (client *Client) Run() error {
	var resp Message
	var reqId int32
	var msg *Message
	var err error
	var procName string

	for {
		if reqId, msg, err = client.RecvMessage(); err != nil {
			return err
		}

		switch msg.MsgType {
		case MsgTypeRequest:
			argv := strings.Split(string(msg.Content), " \t")
			if len(argv) == 0 {
				resp = Message{
					MsgTypeResponse,
					StatusCodeBadArgs,
					StatusStringBadArgs,
					nil,
				}
				client.SendMessage(reqId, &resp)
				continue
			}

			if _, present := client.procRegistry[argv[0]]; !present {
				resp = Message{
					MsgTypeResponse,
					StatusCodeCreateProcErr,
					StatusStringCreateProcErr,
					nil,
				}
				client.SendMessage(reqId, &resp)
				continue
			}

			client.procMap[int(reqId)] = argv[0]

			resp = Message{
				MsgTypeResponse,
				StatusCodeOK,
				StatusStringOK,
				nil,
			}
			client.SendMessage(reqId, &resp)
		case MsgTypeUpdate:
			if procName = client.procMap[int(reqId)]; procName == "" {
				resp = Message{
					MsgTypeResponse,
					StatusCodeProcDead,
					StatusStringProcDead,
					nil,
				}
				client.SendMessage(reqId, &resp)
				continue
			}

			if msg.Code != StatusCodeCall {
				resp = Message{
					MsgTypeResponse,
					StatusCodeBadUpdate,
					StatusStringBadUpdate,
					nil,
				}
				client.SendMessage(reqId, &resp)

			}

			go client.handleRPC(reqId, procName, msg)
		default:
			continue
		}
	}

	return nil
}

func (client *Client) handleRPC(reqId int32, procName string, msg *Message) {
	ret := client.rpcServer.Call(procName, msg.Content)
	resp := Message{
		MsgTypeResponse,
		StatusCodeRet,
		StatusStringRet,
		ret,
	}
	client.SendMessage(reqId, &resp)
}
