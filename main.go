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
	MsgInit        MessageType = "init"
	MsgInitOk      MessageType = "init_ok"
	MsgEcho        MessageType = "echo"
	MsgEchoOk      MessageType = "echo_ok"
	MsgTopology    MessageType = "topology"
	MsgTopologyOk  MessageType = "topology_ok"
	MsgBroadcast   MessageType = "broadcast"
	MsgBroadcastOk MessageType = "broadcast_ok"
	MsgRead        MessageType = "read"
	MsgReadOk      MessageType = "read_ok"
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

type TopologyBody struct {
	BaseBody
	Neighbours map[string][]string `json:"topology,omitempty"`
}

type BroadcastBody struct {
	BaseBody
	Message int `json:"message,omitempty"`
}

type ReadBody struct {
	BaseBody
	Messages []int `json:"messages,omitempty"`
}

type Node struct {
	id         string
	nodeIDs    []string
	neighbours []string
	reader     io.Reader
	writer     io.Writer
	msgID      int
	store      []int
}

func NewNode(nodeID string) *Node {
	return &Node{
		id:         nodeID,
		nodeIDs:    []string{},
		neighbours: []string{},
		reader:     os.Stdin,
		writer:     os.Stdout,
		msgID:      1,
		store:      []int{},
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

	case MsgTopology:
		var body TopologyBody
		if err := json.Unmarshal(bodyBytes, &body); err != nil {
			return nil, err
		}
		return &body, nil

	case MsgBroadcast:
		var body BroadcastBody
		if err := json.Unmarshal(bodyBytes, &body); err != nil {
			return nil, err
		}
		return &body, nil

	case MsgRead:
		var body ReadBody
		if err := json.Unmarshal(bodyBytes, &body); err != nil {
			return nil, err
		}
		return &body, nil

	default:
		return nil, fmt.Errorf("unknown message type: %s", msgType)
	}

}

func (n *Node) handleMessage(msg Message) error {
	log.WithField("node store", n.store).Debug("Data")
	body, ok := msg.Body.(MessageBody)
	if !ok {
		return fmt.Errorf("invalid message body type")
	}

	switch body.GetType() {
	case MsgInit:
		return n.handleInit(msg, body.(*InitBody))
	case MsgEcho:
		return n.handleEcho(msg, body.(*EchoBody))
	case MsgTopology:
		return n.handleTopology(msg, body.(*TopologyBody))
	case MsgBroadcast:
		return n.handleBroadcast(msg, body.(*BroadcastBody))
	case MsgRead:
		return n.handleRead(msg, body.(*ReadBody))
	default:
		return fmt.Errorf("unhandled message type: %s", body.GetType())
	}
}

func (n *Node) handleInit(msg Message, body *InitBody) error {
	log.WithField("node_id", body.NodeID).Info("Received init message")
	n.nodeIDs = body.NodeIDs
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
	log.WithField("echo message", body.Echo).Info("Received echo message")
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

func (n *Node) handleTopology(msg Message, body *TopologyBody) error {
	log.WithField("neighbours", body.Neighbours).Info("Received neighbours message")
	n.neighbours = body.Neighbours[n.id]
	response := &TopologyBody{
		BaseBody: BaseBody{
			Type:    MsgTopologyOk,
			MsgID:   n.nextMsgID(),
			ReplyTo: body.MsgID,
		},
	}
	return n.sendReply(msg.Source, response)
}

func (n *Node) handleBroadcast(msg Message, body *BroadcastBody) error {
	log.WithField("message", body.Message).Info("Received message")
	n.store = append(n.store, body.Message)
	response := &TopologyBody{
		BaseBody: BaseBody{
			Type:    MsgBroadcastOk,
			MsgID:   n.nextMsgID(),
			ReplyTo: body.MsgID,
		},
	}
	return n.sendReply(msg.Source, response)
}

func (n *Node) handleRead(msg Message, body *ReadBody) error {
	response := &ReadBody{
		BaseBody: BaseBody{
			Type:    MsgReadOk,
			MsgID:   n.nextMsgID(),
			ReplyTo: body.MsgID,
		},
		Messages: n.store,
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
	log.SetLevel(logrus.DebugLevel)
	node := NewNode("n1")
	log.Info("Starting node")
	if err := node.Listen(); err != nil {
		log.WithError(err).Fatal("Node failed")
	}
}
