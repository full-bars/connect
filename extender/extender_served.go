package extender

import "github.com/urnetwork/connect"

var servedServiceNames = [...]string{"forward", "gossip", "feed", "other"}

func (self *ExtenderServer) recordServed(service uint32) {
	index := len(servedServiceNames) - 1
	switch service {
	case connect.ExtenderServiceForward, connect.ExtenderServiceGossip, connect.ExtenderServiceFeed:
		index = int(service)
	}
	self.served[index].Add(1)
}

// ServedCounts returns accepted requests handed over to a client stream by service.
func (self *ExtenderServer) ServedCounts() map[string]uint64 {
	counts := map[string]uint64{}
	for index, name := range servedServiceNames {
		if count := self.served[index].Load(); count != 0 {
			counts[name] = count
		}
	}
	return counts
}
