package zmqcluster

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/Arceliar/phony"
	"github.com/stretchr/testify/assert"
)

type testListener struct {
	phony.Inbox
	identity string
	received [][]string
	sent     [][]string
	counter  int
}

func newTestListener(identity string) *testListener {
	return &testListener{
		identity: identity,
		received: [][]string{},
		sent:     [][]string{},
	}
}

func (tl *testListener) OnMessage(identity []byte, message []byte) {
	tl.Act(tl, func() {
		log.Printf("%s: received '%s' from '%s'", tl.identity, string(message), string(identity))
		tl.received = append(tl.received, []string{string(identity), string(message)})
	})
}

func (tl *testListener) OnMessageSent(peer string, message []byte) {
	tl.Act(tl, func() {
		log.Printf("%s: sent '%s' to '%s'", tl.identity, string(message), peer)
		tl.sent = append(tl.sent, []string{peer, string(message)})
	})
}

func (tl *testListener) OnNewPeerConnected(c Cluster, peer string) {
	tl.Act(tl, func() {
		// broadcasting a counter to all peers
		c.BroadcastMessage([]byte(fmt.Sprint(tl.counter)))
		tl.counter++
	})
}

func (tl *testListener) Received() [][]string {
	res := [][]string{}
	phony.Block(tl, func() {
		res = append(res, tl.received...)
	})
	return res
}

func (tl *testListener) WaitForNumberOfMessagesReceivedEq(t *testing.T, expectedCount int) {
	for w := 0; w < 15; w++ {
		if expectedCount == len(tl.Received()) {
			// all ok
			return
		}
		log.Printf("%s: waiting for the number of message to reach %d ...", tl.identity, expectedCount)
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, expectedCount, len(tl.Received()))
}

func TestZmqCluster(t *testing.T) {
	t.Run("updating the cluster", func(t *testing.T) {
		port1 := randomPort()
		l1 := newTestListener("l1")
		c1 := NewZmqCluster("1", "tcp://:"+port1)
		c1.AddListenerSync(l1)
		t.Cleanup(c1.Stop)
		assert.NoError(t, c1.Start())
		// repeated starts are idempotent
		assert.NoError(t, c1.Start())

		// this should go "nowhere" as there are no peers
		c1.BroadcastMessage([]byte("m1"))

		l1.WaitForNumberOfMessagesReceivedEq(t, 0)

		l2 := newTestListener("l2")
		port2 := randomPort()
		c2 := NewZmqCluster("2", "tcp://:"+port2)
		c2.AddListener(l2)
		t.Cleanup(c2.Stop)
		assert.NoError(t, c2.Start())
		l2.WaitForNumberOfMessagesReceivedEq(t, 0)

		// upon c1 discovering a new peer, l1 should broadcast a counter to the new peer
		c1.UpdatePeers([]string{"tcp://localhost:" + port2})
		l2.WaitForNumberOfMessagesReceivedEq(t, 1)
		assert.Equal(t, "1" /*identity*/, string(l2.Received()[0][0]))
		assert.Equal(t, "0" /*message*/, string(l2.Received()[0][1]))

		// the first listener should not have received anything else yet
		l1.WaitForNumberOfMessagesReceivedEq(t, 0)

		// bidirectional connection
		c2.UpdatePeers([]string{"tcp://localhost:" + port1})
		l1.WaitForNumberOfMessagesReceivedEq(t, 1)
		assert.Len(t, l1.sent, 1) // counter from OnNewPeerConnected cb
		assert.Equal(t, "2", string(l1.Received()[0][0]))
		assert.Equal(t, "0", string(l1.Received()[0][1]))

		c1.BroadcastMessage([]byte("broadcast1"))
		// the peer(s) should have received the message
		l2.WaitForNumberOfMessagesReceivedEq(t, 2)
		assert.Equal(t, "1", string(l2.Received()[1][0]))
		assert.Equal(t, "broadcast1", string(l2.Received()[1][1]))
		// c1 should not have received the message
		l1.WaitForNumberOfMessagesReceivedEq(t, 1)
		assert.Len(t, l1.sent, 2)
	})

	t.Run("tcp port parsed from bind string", func(t *testing.T) {
		assert.Equal(t, "4444", tpPortOf(":4444"))
		assert.Equal(t, "3333", tpPortOf("127.0.0.1:3333"))
	})
}
