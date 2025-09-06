package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

type MessageType string

const (
	MsgInit   MessageType = "init"
	MsgInitOk MessageType = "init_ok"
	MsgEcho   MessageType = "echo"
	MsgEchoOk MessageType = "echo_ok"
)

var log = logrus.New()

type Message struct {
	Source      string `json:"src"`
	Destination string `json:"dest"`
	Body        any    `json:"body"`
}

type MessageBody interface {
	GetType() MessageType
	GetMsgID() int
	SetReplyTo(int)
	SetMsgID(int)
}

type BaseBody struct {
	Type    MessageType `json:"type"`
	MsgID   int         `json:"msg_id,omitempty"`
	ReplyTo int         `json:"in_reply_to,omitempty"`
}

func (b *BaseBody) GetType() MessageType { return b.Type }
func (b *BaseBody) GetMsgID() int        { return b.MsgID }
func (b *BaseBody) SetReplyTo(id int)    { b.ReplyTo = id }
func (b *BaseBody) SetMsgID(id int)      { b.MsgID = id }

type InitBody struct {
	BaseBody
	NodeID  string   `json:"node_id,omitempty"`
	NodeIDs []string `json:"node_ids,omitempty"`
}

type EchoBody struct {
	BaseBody
	Echo string `json:"echo"`
}

type Node struct {
	id     string
	reader io.Reader
	writer io.Writer
	msgID  int
}

func NewNode(nodeID string) *Node {
	return &Node{
		id:     nodeID,
		reader: os.Stdin,
		writer: os.Stdout,
		msgID:  1,
	}
}

func (n *Node) nextMsgID() int {
	n.msgID++
	return n.msgID
}

func (n *Node) Listen() error {
	scanner := bufio.NewScanner(n.reader)
	for scanner.Scan() {
		line := scanner.Bytes()
		if err := n.processMessage(line); err != nil {
			log.WithError(err).Error("Failed to process message")
			continue
		}
	}
	return scanner.Err()
}

func (n *Node) processMessage(data []byte) error {
	log.WithField("raw_message", string(data)).Debug("Received message")
	var rawMsg struct {
		Source      string         `json:"src"`
		Destination string         `json:"dest"`
		Body        map[string]any `json:"body"`
	}
	if err := json.Unmarshal(data, &rawMsg); err != nil {
		return fmt.Errorf("failed to unmarshal message:%w", err)
	}

	msgType, ok := rawMsg.Body["type"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid message type")
	}

	body, err := n.parseMessageBody(MessageType(msgType), rawMsg.Body)
	if err != nil {
		return fmt.Errorf("failed to parse message body: %w", err)
	}

	msg := Message{
		Source:      rawMsg.Source,
		Destination: rawMsg.Destination,
		Body:        body,
	}

	return n.handleMessage(msg)
}

func (n *Node) parseMessageBody(msgType MessageType, rawBody map[string]any) (MessageBody, error) {
	bodyBytes, err := json.Marshal(rawBody)
	if err != nil {
		return nil, err
	}

	switch msgType {
	case MsgInit:
		var body InitBody
		if err := json.Unmarshal(bodyBytes, &body); err != nil {
			return nil, err
		}
		return &body, nil

	case MsgEcho:
		var body EchoBody
		if err := json.Unmarshal(bodyBytes, &body); err != nil {
			return nil, err
		}
		return &body, nil

	default:
		return nil, fmt.Errorf("unknown message type: %s", msgType)
	}

}

func (n *Node) handleMessage(msg Message) error {
	body, ok := msg.Body.(MessageBody)
	if !ok {
		return fmt.Errorf("invalid message body type")
	}

	switch body.GetType() {
	case MsgInit:
		return n.handleInit(msg, body.(*InitBody))
	case MsgEcho:
		return n.handleEcho(msg, body.(*EchoBody))
	default:
		return fmt.Errorf("unhandled message type: %s", body.GetType())
	}
}

func (n *Node) handleInit(msg Message, body *InitBody) error {
	log.WithField("node_id", body.NodeID).Info("Received init message")
	response := &InitBody{
		BaseBody: BaseBody{
			Type:    MsgInitOk,
			MsgID:   n.nextMsgID(),
			ReplyTo: body.MsgID,
		},
	}
	return n.sendReply(msg.Source, response)
}

func (n *Node) handleEcho(msg Message, body *EchoBody) error {
	log.WithField("node_id", body.Echo).Info("Received echo message")
	response := &EchoBody{
		BaseBody: BaseBody{
			Type:    MsgEchoOk,
			MsgID:   n.nextMsgID(),
			ReplyTo: body.MsgID,
		},
		Echo: body.Echo,
	}
	return n.sendReply(msg.Source, response)
}

func (n *Node) sendReply(dest string, body MessageBody) error {
	reply := Message{
		Source:      n.id,
		Destination: dest,
		Body:        body,
	}

	data, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("failed to marshal reply: %w", err)
	}

	log.WithField("reply", string(data)).Debug("Sending reply")

	if _, err := n.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write reply: %w", err)
	}

	if _, err := n.writer.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	return nil
}

func main() {
	file, err := os.OpenFile("/home/delta/git/go/maelstrom/maelstrom.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Out = os.Stderr
		panic("Cant create logger")
	}
	log.SetOutput(file)
	node := NewNode("n1")
	log.Info("Starting node")
	if err := node.Listen(); err != nil {
		log.WithError(err).Fatal("Node failed")
	}
}
