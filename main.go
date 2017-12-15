package main

import (
	"flag"
	"log"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"encoding/json"
	"github.com/joeshaw/iso8601"
	"sync"
	"os"
	"fmt"
)

const (
	typeHeartbeat = "heartbeat"
	typeTicker    = "ticker"
	typeLevel2    = "l2update"

	writeWait = 10 * time.Second
	readWait  = 60 * time.Second
)

type Client struct {
	Connection *websocket.Conn
}

type Msg struct {
	Type        string          `json:"type"`
	Sequence    int64           `json:"sequence,omitempty"`
	LastTradeId int64           `json:"last_trade_id,omitempty"`
	ProductId   string          `json:"product_id,omitempty"`
	Time        time.Time       `json:"time,omitempty"`
	TradeId     int64           `json:"trade_id,omitempty"`
	Price       string          `json:"price,omitempty"`
	Side        string          `json:"side,omitempty"`
	LastSize    string          `json:"last_size,omitempty"`
	BestBid     string          `json:"best_bid,omitempty"`
	BestAsk     string          `json:"best_ask,omitempty"`
	Changes     [][]interface{} `json:"changes,omitempty"`
	ReceivedAt  string          `json:"received_at"`
}

var addr = flag.String("addr", "ws-feed.gdax.com", "http service address")
var fileName = flag.String("file", "gdax.txt", "write all received messages to a file")

func NewFromRequest(urlStr string) (*Client, error) {
	c, _, err := websocket.DefaultDialer.Dial(urlStr, nil)
	if err != nil {
		return nil, err
	}

	c.SetWriteDeadline(time.Now().Add(writeWait))
	c.WriteMessage(websocket.TextMessage, []byte(`{"type":"subscribe","product_ids":["BTC-USD"],"channels":["level2","heartbeat","ticker"]}`))

	return &Client{c}, nil
}

func main() {
	flag.Parse()

	u := url.URL{Scheme: "wss", Host: *addr}
	log.Printf("connecting to %s", u.String())

	c, err := NewFromRequest(u.String())
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Connection.Close()

	ch := make(chan []byte)
	logger, err := newLogger(*fileName)
	if err != nil {
		log.Fatal("logger:", err)
	}
	go dispatcher(c, ch, logger)
	reader(c, ch)
	close(ch)
}

func dispatcher(c *Client, ch chan []byte, logger *Logger) {
	defer func() {
		c.Connection.Close()
	}()
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				log.Println("MESSAGING: Could not receive event")
				c.Connection.SetWriteDeadline(time.Now().Add(writeWait))
				c.Connection.WriteMessage(websocket.CloseMessage, nil)
				return
			}
			if err := callMethod(m, logger); err != nil {
				log.Printf("MESSAGING: Error in client: %v", err)
				return
			}
		case t := <-ticker.C:
			c.Connection.SetWriteDeadline(time.Now().Add(writeWait))
			err := c.Connection.WriteMessage(websocket.TextMessage, []byte(t.String()))
			if err != nil {
				log.Printf("MESSAGING: Could not write message, err: %v", err)
				return
			}
		}
	}
}

func reader(c *Client, ch chan []byte) {
	defer func() {
		c.Connection.Close()
	}()

	for {
		c.Connection.SetReadDeadline(time.Now().Add(readWait))
		_, m, err := c.Connection.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				log.Printf("error: %v", err)
			}
			break
		}
		ch <- m
	}
}

func callMethod(message []byte, logger *Logger) error {
	m := &Msg{}
	err := json.Unmarshal(message, m)
	if err != nil {
		return err
	}
	m.ReceivedAt = time.Now().Format(iso8601.Format)
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	if m.Type == typeHeartbeat || m.Type == typeTicker || m.Type == typeLevel2 {
		logger.Write(data)
	}

	return nil
}

// ---------------------------------------------------------------------------------------------------------------------
type Logger struct {
	mutex *sync.Mutex
	file  *os.File
}

func newLogger(fileName string) (*Logger, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	return &Logger{file: f, mutex: &sync.Mutex{}}, nil
}

func (w *Logger) Write(b []byte) {
	w.mutex.Lock()
	w.file.WriteString(fmt.Sprintf("%s\n", string(b)))
	w.mutex.Unlock()
}
