package connect

import (
	"testing"
	"unsafe"
)

func TestP2PSourceMismatchSideTableRelease(t *testing.T) {
	client := &Client{}
	const n = 1200
	packs := make([]*ReceivePack, n)
	items := make([]*receiveItem, n)
	owners := make([]*decodedPackOwner, n)
	for i := range packs {
		if i%2 == 0 {
			owners[i] = inboundDecodedPackOwners.take()
			packs[i] = &owners[i].receivePack
			items[i] = &owners[i].receiveItem
			packs[i].decodedOwner = owners[i]
			items[i].decodedOwner = owners[i]
		} else {
			packs[i] = &ReceivePack{}
			items[i] = &receiveItem{}
		}
		mismatch := newP2PSourceMismatch(client, p2pSourceModeCount)
		mismatch.bindPack(packs[i])
		mismatch.bindItem(items[i])
	}
	for i := range packs {
		switch i % 6 {
		case 0, 1:
			packs[i].messagePoolReturn()
		case 2, 3:
			items[i].messagePoolReturn()
		case 4:
			owners[i].release()
		case 5:
			p2pMismatchFor(items[i]).decoded()
			items[i].messagePoolReturn()
		}
	}
	count := 0
	p2pSourceMismatchTable.Range(func(_, _ any) bool { count++; return true })
	if count != 0 {
		t.Fatalf("side table retains %d entries after release", count)
	}
	stats := client.ReceiveStats()
	if stats.SourceMismatchRejectedCount != 1000 || stats.SourceMismatchDecodedCount != 200 || stats.SourceMismatchCount != n {
		t.Fatalf("unexpected outcomes: %+v", stats)
	}
}

func TestP2PSourceMismatchLayoutSizes(t *testing.T) {
	t.Logf("ReceivePack=%d receiveItem=%d decodedPackOwner=%d header=8", unsafe.Sizeof(ReceivePack{}), unsafe.Sizeof(receiveItem{}), unsafe.Sizeof(decodedPackOwner{}))
}
