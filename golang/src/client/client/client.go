package client

import (
	"bufio"
	"encoding/csv"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/external"
)

const connectionAttempts = 3
const connectionAttemptsDelayMs = 300

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputFile  string
	OutputFile string
}

type Client struct {
	conn    net.Conn
	running atomic.Bool
	config  ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	client.running.Store(true)
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	var err error
	var conn net.Conn

	for range connectionAttempts {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			slog.Warn("Retrying connection...")
			time.Sleep(connectionAttemptsDelayMs * time.Millisecond)
			continue
		}
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	defer client.conn.Close()
	go client.handleSignals()

	if err := client.sendFruitRecords(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	if err := client.recvFruitTop(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	return nil
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	client.conn.Close()
}

func (client *Client) sendFruitRecords() error {
	file, err := os.Open(client.config.InputFile)
	if err != nil {
		slog.Debug("Error while runninging input file", "err", err)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		columns := strings.Split(scanner.Text(), ",")
		fruit := columns[0]
		amount, err := strconv.ParseInt(columns[1], 10, 32)
		if err != nil {
			slog.Debug("Error while parsing fruit record", "err", err)
			return err
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(amount)}
		if err := external.WriteFruitRecord(client.conn, &fruitRecord); err != nil {
			return err
		}

		msgType, err := external.ReadMsgType(client.conn)
		if err != nil {
			slog.Debug("Error while reading ack message", "err", err)
			return err
		}
		if msgType != external.Ack {
			return errors.New("Expected Ack message")
		}
	}

	if err := external.WriteEndOfRecords(client.conn); err != nil {
		return err
	}
	msgType, err := external.ReadMsgType(client.conn)
	if err != nil {
		slog.Debug("Error while reading ack message", "err", err)
		return err
	}
	if msgType != external.Ack {
		return errors.New("Expected Ack message")
	}

	return nil
}

func (client *Client) recvFruitTop() error {
	msgType, err := external.ReadMsgType(client.conn)
	if err != nil {
		slog.Debug("Error while reading message type", "err", err)
		return err
	}
	if msgType != external.FruitTop {
		return errors.New("Expected FruitTop message")
	}

	fruitTop, err := external.ReadFruitTop(client.conn)
	if err != nil {
		slog.Debug("Error while reading FruitTop message", "err", err)
		return err
	}
	if err := external.WriteAck(client.conn); err != nil {
		slog.Debug("Error while writing ack message", "err", err)
		return err
	}

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		slog.Debug("Error while creating output file", "err", err)
		return err
	}
	outputFileWriter := csv.NewWriter(outputFile)

	for _, fruitRecord := range fruitTop {
		line := []string{fruitRecord.Fruit, strconv.Itoa(int(fruitRecord.Amount))}
		if err := outputFileWriter.Write(line); err != nil {
			slog.Debug("Error while writing output file", "err", err)
			return err
		}
	}
	outputFileWriter.Flush()

	return nil
}
