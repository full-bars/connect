package connect

import (
	"testing"

	"github.com/urnetwork/connect/protocol"
	"google.golang.org/protobuf/proto"
)

func forgePackFrame(t *testing.T, sourceId Id, destinationId Id, sequenceId Id, sequenceNumber uint64, head bool, content string) []byte {
	t.Helper()
	msgBytes, err := proto.Marshal(&protocol.SimpleMessage{Content: content})
	if err != nil {
		t.Fatal(err)
	}
	return marshalTransferFrameForTest(t, &protocol.TransferFrame{
		TransferPath: TransferPath{SourceId: sourceId, DestinationId: destinationId}.ToProtobuf(),
		Pack: &protocol.Pack{
			MessageId: NewId().Bytes(), SequenceId: sequenceId.Bytes(),
			SequenceNumber: sequenceNumber, Head: head,
			Frames: []*protocol.Frame{{MessageType: protocol.MessageType_TestSimpleMessage, MessageBytes: msgBytes}},
		},
	})
}

// A pack admitted to the buffer but discarded for lack of a contract was not delivered.
func TestP2PSourceMismatchNoContractRejected(t *testing.T) {
	setSourcePolicy(t, "count", true)
	peer := NewId()
	client, route := newSourcePolicyClient(t, TransportTypeP2p, peer, 2)
	delivered := deliverContent(client)
	forged := NewId()
	route <- forgeBarrierFrame(t, forged, client.ClientId(), "no contract")
	waitP2PSourceStats(t, client, func(stats ClientReceiveStatsSnapshot) bool {
		return stats.SourceMismatchCount == 1
	}, "one mismatch outcome")
	expectNoDelivery(t, delivered)
	if stats := client.ReceiveStats(); stats.SourceMismatchRejectedCount != 1 ||
		stats.SourceMismatchDecodedCount != 0 || stats.SourceMismatchDroppedCount != 0 {
		t.Fatalf("a no-contract pack must count rejected, not decoded: %+v", stats)
	}
}

// A held pack is discarded along with the head that failed its contract check.
func TestP2PSourceMismatchHeldNoContractRejected(t *testing.T) {
	setSourcePolicy(t, "count", true)
	peer := NewId()
	client, route := newSourcePolicyClient(t, TransportTypeP2p, peer, 2)
	delivered := deliverContent(client)
	forged := NewId()
	sequenceId := NewId()
	route <- forgePackFrame(t, forged, client.ClientId(), sequenceId, 1, false, "held")
	route <- forgePackFrame(t, forged, client.ClientId(), sequenceId, 0, true, "head")
	waitP2PSourceStats(t, client, func(stats ClientReceiveStatsSnapshot) bool {
		return stats.SourceMismatchCount == 2
	}, "two mismatch outcomes")
	expectNoDelivery(t, delivered)
	if stats := client.ReceiveStats(); stats.SourceMismatchRejectedCount != 2 ||
		stats.SourceMismatchDecodedCount != 0 || stats.SourceMismatchDroppedCount != 0 {
		t.Fatalf("held and head no-contract packs must count rejected: %+v", stats)
	}
}
