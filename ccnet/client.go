// Package ccnet implements basic client library to ccnet server.

package ccnet

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/killing/searpc"
	"log"
	"net"
	"sync"
	"bytes"
)

const (
	unixSockName string = "ccnet.sock"
)

const (
	StatusCodeOK   string = "200"
	StatusStringOK string = "OK"
)

const (
	protocolVersion uint8 = 1

	MsgTypeRequest  uint8 = 2
	MsgTypeUpdate   uint8 = 3
	MsgTypeResponse uint8 = 4
)

var (
	BadProtocolVersion = errors.New("ccnet: bad protocol version")
	BadMessageFormat = errors.New("ccnet: bad message format")
)

// Message is sent and recieved by the client
type Message struct {
	MsgType uint8
	Code    string
	CodeMsg string
	Content []byte
}

func (msg *Message) len() uint16 {
	var ret uint16

	switch msg.MsgType {
	case MsgTypeRequest:
		return uint16(len(msg.content))
	case MsgTypeUpdate, MsgTypeResponse:
		ret = len(msg.Code)
		if msg.CodeMsg != nil {
			ret += (1 + len(msg.CodeMsg))
		}
		ret += (1 + len(msg.Content))
		return ret
	default:
		return 0
	}
}

// Client manages a connection to the ccnet server
type Client struct {
	conn         net.Conn          // connection to ccnet server
	rw           *bufio.ReadWriter // buffer wrapped around the connection.
	procRegistry map[string]int    // Record whether a processor name is registered.
	rpcServer    *searpc.Server
	reqId        int // auto-increment request id.
}

func NewClient(confDir string, rpcServer *searpc.Server) *Client {
	unixSockPath := confDir + "/" + unixSockName
	if conn, err := net.Dial("unix", unixSockPath); err != nil {
		log.Println(err.Error())
		return nil
	}

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	return &Client{conn: conn, rw: rw, procRegistry: make(map[string]int), rpcServer: rpcServer}
}

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
	if msg.msgType != msgTypeResponse || msg.code != StatusCodeOK {
		errMsg = "ccnet: Received bad response for receive-cmd processor"
		return errors.New(errMsg)
	}

	msg = &Message{
		MsgType: MsgTypeUpdate,
		Code:    StatusCodeOK,
		CodeMsg: StatusStringOK,
		Content: []byte("register-service " + procName + " rpc-inner"),
	}
	if err = client.SendMessage(reqId, msg); err != nil {
		return err
	}

	if _, msg, err = client.RecvMessage(); err != nil {
		return err
	}
	if msg.msgType != msgTypeResponse || msg.code != StatusCodeOK {
		errMsg = "ccnet: Received bad response for receive-cmd processor"
		return errors.New(errMsg)
	}

	return nil
}

func (client *Client) NextReqId() (reqId int) {
	reqId = client.reqId
	client.reqId++
	return
}

func (client *Client) writeHeader(msgType uint8, reqId int, contentLen uint16) error {
	var err error

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

	switch msg.MsgType {
	case MsgTypeRequest:
		err = client.rw.WriteString(msg.Content)
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
			_, err = client.rw.WriteString(msg.Content)
		}

		return err
	default:
		log.Panic("ccnet: invalid message type.")
	}
}

func (client *Client) SendMessage(reqId int, msg *Message) error {
	var err error

	if err = client.writeHeader(msg.MsgType, reqId, msg.len()); err != nil {
		return err
	}

	if err = client.writeMessage(msg); err != nil {
		return err
	}

	err = client.rw.Flush()

	return err
}

func (client *Client) RecvMessage() (reqId int, msg *Message, err error) {
	if msgType, reqId, msgLen, err = client.readHeader(); err != nil {
		return
	}

	msg, err = client.readMessage(msgType, msgLen)

	return
}

func (client *Client) readHeader() (msgType uint8, reqId int, msgLen uint16, err error) {
	if version, err = client.rw.ReadByte(); err != nil {
		return
	}
	if version != protocolVersion {
		err = BadProtocolVersion
		return
	}

	if msgType, err = client.rw.ReadByte(); err != nil {
		return
	}

	if err = binary.Read(client.rw, binary.BigEndian, &msgLen); err != nil {
		return
	}

	if err = binary.Read(client.rw, binary.BigEndian, &reqId); err != nil {
		return
	}

	return
}

func (client *Client) readMessage(msgType uint8, msgLen uint16) (msg *Message, err error) {
	var buf = make(byte, msgLen)
	var code, codeMsg string
	var content []byte

	if _, err = ReadFull(client.rw, buf); err != nil {
		return err
	}

	switch msgType {
	case MsgTypeRequest:
		msg = &Message{MsgType: msgType, Content: buf}
	case MsgTypeUpdate, MsgTypeResponse:
		if msgLen < 4 {
			err = BadMessageFormat
			return
		}
		code = string(buf[:3])
		index := bytes.IndexByte(buf[3:], '\n')
		if index == -1 {
			err = BadMessageFormat
			return
		}
		if buf[3] == ' ' {
			codeMsg = string(buf[4:index])
		}
		if msgLen > index + 1 {
			content = make(byte, msgLen-index-1)
			copy(content, buf[index+1:])
		}
		msg = &Message{msgType, code, codeMsg, content}
	}
	return
}
