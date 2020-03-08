package tendStar

import (
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"math"
	"strconv"
	"strings"
	"sync"
	)

// Replica for one Tendermint instance
type Replica struct {
	paxi.Node
	*Tendermint
	mux sync.Mutex
}
const (
	HTTPHeaderSlot       = "Slot"
	HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute    = "Execute"
	HTTPHeaderInProgress = "Inprogress"
)
// NewReplica generates new Pbft replica
func NewReplica(id paxi.ID) *Replica {
	log.Debugf("Replica started \n")
	r := new(Replica)

	r.Node = paxi.NewNode(id)
	r.Tendermint = NewTendermint(r)

	r.Register(paxi.Request{},  r.handleRequest)
	r.Register(Propose{},       r.handlePropose)
	r.Register(PreVote{},       r.HandlePreVote)
	r.Register(PreCommit{},     r.HandlePreCommit)
	r.Register(ActPropose{},    r.HandleActPropose)
	r.Register(ActPreCommit{},  r.HandleActPreCommit)
	r.Register(ActPreVote{},  r.HandleActPreVote)

	return r
}
func (p *Replica) handleRequest(m paxi.Request) {
	log.Debugf("\n<-----------handleRequest----------->\n")
	if p.slot <= 0 {
		fmt.Print("-------------------tendStar-------------------------")
	}
	p.slot++ // slot number for every request
	if p.slot % 100 == 0 {
		fmt.Print("p.slot", p.slot)
	}
	log.Debugf("Node's ID:%v, command:%v ", p.ID(), m.Command.Key)

	log.Debugf("p.slot %v", p.slot)
	p.Requests = append(p.Requests, &m)

	e, ok := p.logR[p.slot]
	if !ok {
		p.logR[p.slot] = &RequestSlot{
			request:     &m,
			slot:		 p.slot,
			RecReqst:    paxi.NewQuorum(),
			commit:      false,
			count:       0,
			active:      false,
			Leader:      false,
		}
	}
	e, ok = p.logR[p.slot]

	if e.Leader == false {
		num := int(math.Mod(p.quorum.INC(), float64(p.quorum.Total())))
		num = num + 1
		var s string = string(p.Tendermint.ID())
		var b string
		if strings.Contains(s, ".") {
			split := strings.SplitN(s, ".", 2)
			b = split[1]
		} else {
			b = s
		}
		i, _ := strconv.Atoi(b)
		log.Debugf("<<<<<<<<<<<<<<<< num >>>>>>>>>>>>>>>>>> %v, i%v, p.slot:%v", num, i, p.slot)
		if i == num {
			e.Leader = true
			p.ballot.Next(p.ID())
			log.Debugf("p.ballot %v ", p.ballot)
			log.Debugf("<<<<<<<<<<<<<<<<<< The leader for >>>>>>>>>>>>>>>>>>>>> %v ", i)
			p.Tendermint.view.Next(p.ID())
		}else{
			e.request = &m
		}
	}
	if e.Leader == true {
		e.commit = true
		log.Debugf("p.Requests[%v]: %v", e.slot, e.request)
		p.Tendermint.HandleRequest(*e.request, e.slot)
	}else{
		log.Debugf(" Late message and this will be executed")
			p.HandlePreCommit(PreCommit{
				Ballot:     p.ballot,
				ID:         p.ID(),
				Request:    m,
				Command:    m.Command,
				Slot:       p.slot,
				Commit:     false,
			})
		}
}