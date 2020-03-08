package pbft

import (
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"math"
	"strconv"
	"strings"
	"time"
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

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(PrePrepare{},   r.HandlePre)
	r.Register(Prepare{},      r.HandlePrepare)
	r.Register(Commit{},       r.HandleCommit)

	return r
}

func (p *Replica) handleRequest(m paxi.Request) {
	log.Debugf("<---------------------handleRequest------------------------->")
	if p.slot == 0 {
		fmt.Print("-------------------PBFT-------------------------")
	}
	p.slot++ // slot number for every request
	if p.slot % 100 == 0 {
		fmt.Print("p.slot", p.slot)
	}
	log.Debugf("p.slot %v", p.slot)
	p.requests = append(p.requests, &m)

	e, ok := p.logR[p.slot]
	if !ok {
		p.logR[p.slot] = &RequestSlot{
			request:     &m,
			//slot:        p.slot,
			commit:      false,
			active:      false,
			//Concurrency: 0,
			Leader:      false,
			RecReqst:	 paxi.NewQuorum(),
			MissReq:	&m,
		}
	}
	e, ok = p.logR[p.slot]
	p.RecivedReq = true
	e.active = true
	e.request = &m

	_, ok1 := p.log[p.slot]
	if !ok1 {
		p.log[p.slot] = &entry{
			ballot:    p.ballot,
			view:      p.view,
			command:   m.Command,
			commit:    false,
			request:   &m,
			timestamp: time.Now(),
			Digest:    GetMD5Hash(&m),
			Q1:        paxi.NewQuorum(),
			Q2:        paxi.NewQuorum(),
			Q3:        paxi.NewQuorum(),
			Q4:        paxi.NewQuorum(),
		}
	}
	_, ok1 = p.log[p.slot]

	if p.activeView == true{
		log.Debugf("The view leader : %v ", p.ID())
		log.Debugf("The requests[%v] : %v ",p.slot, p.Pbft.requests[p.slot])
		e.Leader = true
		p.Pbft.HandleRequest(*p.Pbft.requests[p.slot],p.slot)
	}

	if e.Leader == false && !p.activeView {
		num := int(math.Mod(p.quorum.INC(), float64(p.quorum.Total())))
		num = 1
		var s string = string(p.Pbft.ID())
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
			p.activeView = true
			p.view.Next(p.ID())
			log.Debugf("The view leader : %v ", p.ID())
			log.Debugf("The requests[%v] : %v ", p.slot, p.Pbft.requests[p.slot])
			p.Pbft.HandleRequest(*p.Pbft.requests[p.slot], p.slot)
		} else {
			for s := p.execute; s <= p.slot; s++ {
				if p.log[s].commit == true {
					log.Debugf("Check if the request is late")
					p.HandleCommit(Commit{
						Ballot:  p.ballot,
						ID:      p.ID(),
						View:    p.view,
						Slot:    s,
						Digest:  GetMD5Hash(&m),
						Command: m.Command,
						Request: m,
					})
				}
			}
		}
	}
}