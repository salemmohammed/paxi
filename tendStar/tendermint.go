package tendStar

import (
	//"container/list"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"strconv"
	"time"
	"sync"
)
// log's entries
type entry struct {

	Ballot     	  paxi.Ballot
	commit     	  bool
	request    	  *paxi.Request
	Timestamp  	  time.Time
	command    	  paxi.Command

	Q1	  		  *paxi.Quorum
	Q2    		  *paxi.Quorum
	Q3    		  *paxi.Quorum

	active 		  bool
	Precommitmsg  bool

}

type RequestSlot struct {

	request    	  *paxi.Request
	RecReqst	  *paxi.Quorum
	commit     	  bool
	count 		  int
	Neibors  	  []paxi.ID
	active 		  bool
	Concurrency   int
	Leader		  bool
	slot		  int

}

type Tendermint struct {

	paxi.Node
	log      					map[int]*entry 				// log ordered by slot
	logR      					map[int]*RequestSlot 		// log ordered by slot for receiving requests
	config 						[]paxi.ID
	execute 					int             			// next execute slot number
	active  					bool		    			// active leader
	ballot  					paxi.Ballot     			// highest ballot number
	slot    					int             			// highest slot number
	quorum   					*paxi.Quorum    			// phase 1 quorum
	Requests 					[]*paxi.Request 			// phase 1 pending requests
	view     					paxi.View 	    			// view number
	Member						*paxi.Memberlist
	MyCommand       			paxi.Command
	MyRequests					*paxi.Request
	TemID						paxi.ID
	RequestFlag	   				bool
	c              				chan paxi.Request
	done           				chan bool
	ChooseID	  				paxi.ID
	count 		   				int
	Created						bool
	Leader						bool
	EarlyPropose				bool
	mux 						sync.Mutex
	Plist						[]paxi.ID

}
// NewPaxos creates new paxos instance
func NewTendermint(n paxi.Node, options ...func(*Tendermint)) *Tendermint {

	p := &Tendermint{
		Node:          	 	n,
		log:           	 	make(map[int]*entry, paxi.GetConfig().BufferSize),
		logR:           	make(map[int]*RequestSlot, paxi.GetConfig().BufferSize),
		slot:          	 	-1,
		quorum:        	 	paxi.NewQuorum(),
		Requests:      	 	make([]*paxi.Request, 0),
		Member:         	paxi.NewMember(),
		RequestFlag: 		false,
		count:				0,
		EarlyPropose:		false,
		Leader:				false,
		Plist:				make([]paxi.ID,0),
	}
	for _, opt := range options {
		opt(p)
	}
	return p

}
// HandleRequest handles request and start phase 1 or phase 2
func (p *Tendermint) HandleRequest(r paxi.Request, s int) {

	log.Debugf("\n<---R----HandleRequest----R------>\n")
	log.Debugf("Sender ID %v, slot=%v", r.NodeID, s)

	p.logR[s].active = true

	p.log[s] = &entry{
		Ballot:    	p.ballot,
		commit:    	false,
		request:   	&r,
		Timestamp: 	time.Now(),
		Q1:			paxi.NewQuorum(),
		Q2: 		paxi.NewQuorum(),
		Q3: 		paxi.NewQuorum(),
		command:    r.Command,
		active:     p.logR[s].active,
	}

	p.Broadcast(Propose{
		Ballot:     p.ballot,
		ID:         p.ID(),
		Request:    *p.log[s].request,
		View:       p.view,
		Slot:       s,
		Command:    p.log[s].command,
	})
}
// handle propose from primary
func (p *Tendermint) handlePropose(m Propose) {
	log.Debugf("\n<-------P-------------handlePropose--------P---------->\n")
	log.Debugf("m.slot %v", m.Slot)
	log.Debugf("sender %v", m.ID)
	if m.Ballot > p.ballot {
		log.Debugf("m is bigger m.Ballot:%v, p.ballot:%v", m.Ballot, p.ballot)
		p.ballot = m.Ballot
	}
	_, ok := p.log[m.Slot]
	if !ok {
		log.Debugf("Create the log")
		p.log[m.Slot] = &entry{
			Ballot:     p.ballot,
			commit:     false,
			request:    &m.Request,
			Timestamp:  time.Now(),
			command:    m.Command,
		}
	}
	_, ok = p.log[m.Slot]
	p.Send(m.ID, ActPropose{
		Ballot:     p.ballot,
		ID:         p.ID(),
		Slot:       m.Slot,
		Request:    m.Request,
		Command:    m.Command,
	})
	log.Debugf("++++++++++++++++++++++++++ handlePropose Done ++++++++++++++++++++++++++")
}

func (p *Tendermint) HandleActPropose(m ActPropose) {
	log.Debugf("\n\n\n<---------V-----------HandleActPropose----------V-------->")
	log.Debugf("m.slot %v", m.Slot)
	log.Debugf("sender %v", m.ID)
	if m.Ballot > p.ballot {
		log.Debugf("m.ballot is bigger")
		p.ballot = m.Ballot
	}
	e, ok := p.log[m.Slot]
	if !ok{
		return
	}
	e.Q1.ACK(m.ID)
	if p.logR[m.Slot].active && e.Q1.Majority(){
		e.Q1.Reset()
		p.Broadcast(PreVote{
		Ballot:     p.ballot,
		ID:         p.ID(),
		Slot:       m.Slot,
		Request:    m.Request,
		Command:    m.Command,
	})
}
}

func (p *Tendermint) HandlePreVote(m PreVote) {
	log.Debugf("\n\n\n<---------V-----------HandlePreVote----------V-------->")
	log.Debugf("m.slot %v", m.Slot)
	log.Debugf("sender %v", m.ID)

	if m.Ballot > p.ballot {
		log.Debugf("m.ballot is bigger")
		p.ballot = m.Ballot
	}

	p.Send(m.ID, ActPreVote{
		Ballot:     p.ballot,
		ID:         p.ID(),
		Slot:       m.Slot,
		Request:    m.Request,
		Command:    m.Command,
	})
}

func (p *Tendermint) HandleActPreVote(m ActPreVote) {
	log.Debugf("\n\n\n<---------V-----------HandleActPreVote----------V-------->")
	log.Debugf("m.slot %v", m.Slot)
	log.Debugf("sender %v", m.ID)

	if m.Ballot > p.ballot {
		log.Debugf("m.ballot is bigger")
		p.ballot = m.Ballot
	}
	e, ok := p.log[m.Slot]
	if !ok{
		return
	}
	e.Q2.ACK(m.ID)
	if p.logR[m.Slot].active && e.Q2.Majority(){
		e.Q2.Reset()
		p.Broadcast(PreCommit{
			Ballot:     p.ballot,
			ID:         p.ID(),
			Slot:       m.Slot,
			Request:    m.Request,
			Command:    m.Command,
			Commit:		true,
		})
	}
}

func (p *Tendermint) HandlePreCommit(m PreCommit) {
	log.Debugf("\n\n<----------C----------PreCommit--------C---------->")
	log.Debugf("m.slot %v", m.Slot)
	log.Debugf("sender %v", m.ID)
	if m.Ballot > p.ballot {
		log.Debugf("m.ballot is bigger")
		p.ballot = m.Ballot
	}

	if p.ID() != m.ID{
		p.Send(m.ID, ActPreCommit{
			Ballot:  p.ballot,
			ID:      p.ID(),
			Slot:    m.Slot,
			Request: m.Request,
			Command: m.Command,
		})
	}

	e, ok := p.log[m.Slot]
	if !ok{
		log.Debugf("failed p.log")
		return
	}
	if m.Commit == false && e.commit == false{
		log.Debugf("return")
		return
	}
	e.commit = true
	_, ok1 := p.logR[m.Slot]
	if !ok1{
		return
	}
	if e.commit == true{
		p.exec()
	}
}
func (p *Tendermint) HandleActPreCommit(m ActPreCommit) {
	log.Debugf("\n\n<----------C----------HandlePreCommit--------C---------->")
	log.Debugf("m.slot %v", m.Slot)
	log.Debugf("sender %v", m.ID)
	if m.Ballot > p.ballot {
		log.Debugf("m is bigger than p")
		p.ballot = m.Ballot
	}
	e, ok := p.log[m.Slot]
	if !ok {
		return
	}
	e.Q3.ACK(m.ID)
	if e.Q3.Majority() {
		e.commit = true
		e.Q3.Reset()
		p.exec()
	}
}
func (p *Tendermint) exec() {
		log.Debugf("<--------------------exec()------------------>")
		for {
			log.Debugf("p.execute %v", p.execute)
			e, ok := p.log[p.execute]
			if !ok{
				return
			}
			if !ok || !e.commit {
				log.Debugf("BREAK")
				break
			}
			value := p.Execute(e.command)

			if e.request != nil && e.active {
				reply := paxi.Reply{
					Command:    e.request.Command,
					Value:      value,
					Properties: make(map[string]string),
				}
				reply.Properties[HTTPHeaderSlot] = strconv.Itoa(p.execute)
				reply.Properties[HTTPHeaderBallot] = e.Ballot.String()
				reply.Properties[HTTPHeaderExecute] = strconv.Itoa(p.execute)
				e.request.Reply(reply)
				log.Debugf("********* Reply Primary *********")
				e.request = nil
				e.active = false
				p.view.Reset(p.ID())
			}

			if p.Leader == false {
				d, d1 := p.logR[p.execute]
				if !d1{
					log.Debugf("d is nil")
					break
				}
				if e.request != nil && !e.active {
					log.Debugf("********* Replica Request ********* ")
					p.mux.Lock()
					reply := paxi.Reply{
						Command:    d.request.Command,
						Value:      value,
						Properties: make(map[string]string),
					}
					p.mux.Unlock()
					reply.Properties[HTTPHeaderSlot] = strconv.Itoa(p.execute)
					reply.Properties[HTTPHeaderBallot] = e.Ballot.String()
					reply.Properties[HTTPHeaderExecute] = strconv.Itoa(p.execute)
					d.request.Reply(reply)
					d.request = nil
					log.Debugf("********* Reply Replicas *********")
				}
			}
			// TODO clean up the log periodically
			delete(p.log, p.execute)
			delete(p.logR, p.execute)
			p.execute++
		}
}