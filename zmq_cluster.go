package percounter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Arceliar/phony"
	"github.com/go-zeromq/zmq4"
)

type ClusterListener interface {
	OnMessage(message []byte)
	OnNewPeerConnected(c Cluster, peer string)
}

type Cluster interface {
	UpdatePeers(peers []string)
	SendMessageToPeer(peer string, message []byte)
	BroadcastMessage(message []byte)
	Start() error
	Stop()
	AddListenerSync(listener ClusterListener)
	AddListener(listener ClusterListener)
}

type ZmqCluster struct {
	phony.Inbox
	listeners         []ClusterListener
	bindAddr          string
	server            zmq4.Socket
	peers             map[string]zmq4.Socket
	started           bool
	ctx               context.Context
	stop              context.CancelFunc
	receiveLaterDelay time.Duration
	myIdentity        string
}

func NewZmqCluster(identity, bindAddr string) *ZmqCluster {
	ctx, cancel := context.WithCancel(context.Background())
	res := &ZmqCluster{
		bindAddr:          bindAddr,
		listeners:         []ClusterListener{},
		server:            zmq4.NewPull(ctx),
		peers:             make(map[string]zmq4.Socket),
		ctx:               ctx,
		stop:              cancel,
		myIdentity:        identity,
		receiveLaterDelay: 100 * time.Millisecond,
	}
	return res
}

func (z *ZmqCluster) Start() error {
	var err error
	phony.Block(z, func() {
		if z.started {
			log.Printf("%s: already started", z.myIdentity)
			return
		}
		err = z.server.Listen(z.bindAddr)
		if err != nil {
			err = fmt.Errorf("could not start listening at %s: %v", z.bindAddr, err)
			return
		}
		z.started = true
		go z.receiveLoop()
	})
	return err
}

func (z *ZmqCluster) Stop() {
	// safe to call from any goroutine
	z.stop()
	// this must be synchronized
	phony.Block(z, func() {

		log.Printf("%s: disconnecting", z.myIdentity)
		z.server.Close()
	})
}

func (z *ZmqCluster) AddListenerSync(listener ClusterListener) {
	phony.Block(z, func() {
		z.listeners = append(z.listeners, listener)
	})
}

func (z *ZmqCluster) AddListener(listener ClusterListener) {
	z.Act(z, func() {
		z.listeners = append(z.listeners, listener)
	})
}

// UpdatePeers adds new connections and removes ones not present in the input
func (z *ZmqCluster) UpdatePeers(peers []string) {
	z.Act(z, func() {
		newPeers := setOf(peers)

		// if not in new peers, close & remove the connection
		for clientPeer, conn := range z.peers {
			if _, ok := newPeers[clientPeer]; !ok {
				log.Printf("%s: removing connection to %s", z.myIdentity, clientPeer)
				conn.Close()
				delete(z.peers, clientPeer)
			}
		}

		// if not connected, connect
		for _, peer := range peers {
			if _, ok := z.peers[peer]; !ok {
				socket := zmq4.NewPush(context.Background())
				log.Printf("%s: connecting to %s", z.myIdentity, peer)
				err := socket.Dial(peer)
				if err != nil {
					log.Printf("%s: could not connect to peer: %s: %v", z.myIdentity, peer, err)
					continue
				}
				z.peers[peer] = socket
				for _, listener := range z.listeners {
					listener.OnNewPeerConnected(z, peer)
				}
			}
		}
	})
}

func (z *ZmqCluster) BroadcastMessage(message []byte) {
	z.Act(z, func() {
		if len(z.peers) == 0 {
			log.Printf("%s: no peers in cluster, ignoring message", z.myIdentity)
			return
		}

		for _, client := range z.peers {
			err := z.sendToPeerSync(client, message)
			if err != nil {
				log.Printf("%s: error sending state state: %v", z.myIdentity, err)
				continue
			}
		}
	})
}

func (z *ZmqCluster) SendMessageToPeer(peer string, message []byte) {
	z.Act(z, func() {
		client, ok := z.peers[peer]
		if !ok {
			log.Printf("%s: client not found for peer %s", z.myIdentity, peer)
			return
		}
		err := z.sendToPeerSync(client, message)
		if err != nil {
			log.Printf("%s: failed sending a message to peer %s: %v", z.myIdentity, peer, err)
		}
	})
}

func (z *ZmqCluster) sendToPeerSync(client zmq4.Socket, message []byte) error {
	return client.Send(zmq4.NewMsg(message))
}

func (z *ZmqCluster) receiveLoop() {
	log.Printf("%s: started listening to incoming messages at %s", z.myIdentity, z.bindAddr)
	for {
		select {
		case <-z.ctx.Done():
			log.Printf("%s: stopped listening to incoming messages at %s", z.myIdentity, z.bindAddr)
			z.started = false
			return
		default:
		}

		msg, err := z.server.Recv()
		if err != nil {
			log.Printf("%s: failed receive: %v", z.myIdentity, err)
			if !errors.Is(err, context.Canceled) {
				go z.receiveLater()
			} else {
				log.Printf("%s: stopping receive loop", z.myIdentity)
			}
			return
		}
		for _, listener := range z.listeners {
			listener.OnMessage(msg.Bytes())
		}
	}
}

func (z *ZmqCluster) receiveLater() {
	time.Sleep(z.receiveLaterDelay)
	z.receiveLoop()
}

func setOf(s []string) map[string]bool {
	var res = make(map[string]bool)
	for _, e := range s {
		res[e] = true
	}
	return res
}
