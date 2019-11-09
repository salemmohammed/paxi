package pbft

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"strconv"
	"strings"
)

const (
	HTTPHeaderSlot       = "Slot"
	HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute    = "Execute"
)

// Replica for one Pbft instance
type Replica struct {
	paxi.Node
	*Pbft
}

// NewReplica generates new Pbft replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Pbft = NewPbft(r)
	r.Register(paxi.Request{}, r.handleView)
	r.Register(ViewChange{}, r.handleLeaderChange)
	r.Register(LeaderChange{}, r.handleRequest)
	r.Register(PrePrepare{}, r.HandlePre)
	r.Register(Prepare{}, r.HandlePrepare)
	r.Register(Commit{}, r.HandleCommit)
	return r
}

func (r *Replica) handleView(m paxi.Request) {
	log.Debugf("<---------------------handleView------------------------->")

	for id := range paxi.GetConfig().Addrs {
		r.committed[id] 	= 	0
		r.prepared[id]  	= 	0
		r.preprepared[id] 	= 	0
	}

	r.keys = make([]paxi.ID, 0)

	if !r.Pbft.IsLeader(r.Pbft.ID()){
		r.Pbft.NonPrimaryRequest(m)
	}

	log.Debugf("r.Pbft.activeView=%v",r.Pbft.activeView)

	r.Pbft.requests = nil
	r.Pbft.requests = append(r.Pbft.requests, &m)

	if !r.Pbft.activeView{
		log.Debugf("The begining of the protocol")
		log.Debugf("r.Pbft.view=%v",r.Pbft.view)
		log.Debugf("r.Pbft.requests  %v ",r.Pbft.requests[0])
		r.Pbft.Broadcast(ViewChange{
			View:r.Pbft.view,
			ID: r.Pbft.ID(),
			Request: m,
		})
	}else{
		r.Pbft.HandleRequest(*r.Pbft.requests[0])
	}
}

func (r *Replica) handleLeaderChange(m ViewChange) {
	log.Debugf("<--------------------handleLeaderChange--------------------------->")
	log.Debugf("ViewChange received=%v", m)
	r.Pbft.VQ.ACK(m.ID)
	r.Pbft.VQ.ACK(r.Pbft.ID())
	if !r.Pbft.QView(r.Pbft.VQ){
		log.Debugf("return oview")
		return
	}
	log.Debugf("r.Pbft.VQ.Size()=%v", r.Pbft.VQ.Size())

	if r.Pbft.VQ.All() {
		min := r.Pbft.VQ.IDs()
		log.Debugf("min=%v", min)
		var s string = string(r.Pbft.ID())
		var b string
		if strings.Contains(s, ".") {
			split := strings.SplitN(s, ".", 2)
			b = split[1]
		}else{
			b = s
		}
		i, _ := strconv.Atoi(b)
		if i == min {
			for i := 0; i < min; i++ {
				r.Pbft.view.Next(r.Pbft.ID())
				log.Debugf("r.Pbft.view=%v", r.Pbft.view)
				log.Debugf("r.Pbft.view.ID()=%v", r.Pbft.view.ID())
			}
		}else{
			log.Debugf("This replica is not the minimum=%v", r.Pbft.view)
		}

		log.Debugf("The P.ID who are still running %v", r.Pbft.ID())
		if r.Pbft.ID() == r.Pbft.view.ID() {
			log.Debugf("Replica that carry the view min will be the leader=%v", r.Pbft.ID())
			log.Debugf("r.Pbft.view=%v", r.Pbft.view)
			r.Pbft.activeView = true
			r.Pbft.VQ.Reset()
			//log.Debugf("r.Request=%v", *r.Pbft.requests[0])

			r.Pbft.Broadcast(LeaderChange{
				View:    r.Pbft.view,
				ID:      r.Pbft.ID(),
				})

			}else{
			r.Pbft.VQ.Reset()
		}
	}
	if r.Pbft.ID() == r.Pbft.view.ID() && r.Pbft.activeView {
		r.Pbft.HandleRequest(*r.Pbft.requests[0])
	}
}

func (r *Replica) handleRequest(m LeaderChange) {
	log.Debugf("<--------------------handleRequest--------------------------->")
	log.Debugf("Replica %v ", r.ID())
	r.Pbft.view = m.View
	log.Debugf("r.Pbft.activeView=%v",r.Pbft.activeView)
	r.Pbft.activeView = true
	log.Debugf("r.Pbft.activeView=%v",r.Pbft.activeView)
	log.Debugf("view for all replica is %v ", r.Pbft.view)
}