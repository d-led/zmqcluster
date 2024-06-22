package percounter

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
	received [][]byte
	counter  int
}

func newTestListener() *testListener {
	return &testListener{
		received: [][]byte{},
	}
}

func (tl *testListener) OnMessage(message []byte) {
	tl.Act(tl, func() {
		tl.received = append(tl.received, message)
	})
}

func (tl *testListener) OnNewPeerConnected(c Cluster, peer string) {
	tl.Act(tl, func() {
		c.BroadcastMessage([]byte(fmt.Sprint(tl.counter)))
		tl.counter++
	})
}

func (tl *testListener) Received() [][]byte {
	res := [][]byte{}
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
		log.Printf("waiting for the number of message to reach %d ...", expectedCount)
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, expectedCount, len(tl.Received()))
}

func TestZmqCluster(t *testing.T) {
	t.SkipNow()
	t.Run("updating the cluster", func(t *testing.T) {
		port1 := randomPort()
		port2 := randomPort()
		l1 := newTestListener()
		c1 := NewZmqCluster("1", "tcp://:"+port1)
		c1.AddListenerSync(l1)
		t.Cleanup(c1.Stop)
		assert.NoError(t, c1.Start())
		// no repeated starts
		assert.Error(t, c1.Start())

		// this should go "nowhere" as there are no peers
		c1.BroadcastMessage([]byte("m1"))

		l1.WaitForNumberOfMessagesReceivedEq(t, 0)

		l2 := newTestListener()
		c2 := NewZmqCluster("2", "tcp://:"+port2)
		c2.AddListener(l2)
		t.Cleanup(c2.Stop)
		assert.NoError(t, c2.Start())
		l2.WaitForNumberOfMessagesReceivedEq(t, 0)

		// upon c1 discovering a new peer, l1 should broadcast a counter to the new peer
		c1.UpdatePeers([]string{"tcp://localhost:" + port2})
		l2.WaitForNumberOfMessagesReceivedEq(t, 1)
		assert.Equal(t, "0", string(l2.Received()[0]))

		// the first listener should not have received anything else yet
		l1.WaitForNumberOfMessagesReceivedEq(t, 0)

		// bidirectional connection
		c2.UpdatePeers([]string{"tcp://localhost:" + port1})
		l1.WaitForNumberOfMessagesReceivedEq(t, 1)
		assert.Equal(t, "0", string(l1.Received()[0]))

		c1.BroadcastMessage([]byte("broadcast1"))
		// the peer(s) should have received the message
		l2.WaitForNumberOfMessagesReceivedEq(t, 2)
		assert.Equal(t, "broadcast1", string(l2.Received()[1]))
		// c1 should not have received the message
		l1.WaitForNumberOfMessagesReceivedEq(t, 1)
	})
}
