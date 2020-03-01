package tendermint

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

	ID_LIST_PR	  *paxi.Quorum
	ID_LIST_PV    *paxi.Quorum
	ID_LIST_PC    *paxi.Quorum

	active 		  bool
}

type RequestSlot struct {
	request    	  *paxi.Request
	RecReqst	  *paxi.Quorum
	commit     	  bool
	count 		  int
	Neibors  	  []paxi.ID
	active 		  bool
	Concurrency   int
	Leader		 bool
	slot		 int
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
		ID_LIST_PR:	paxi.NewQuorum(),
		ID_LIST_PV: paxi.NewQuorum(),
		ID_LIST_PC: paxi.NewQuorum(),
		command:   r.Command,
		active:    p.logR[s].active,
	}
	e, _ := p.log[s]
	p.Member.Addmember(r.NodeID)
	p.count = 0

	for _, v := range p.Member.Neibors {
		p.count++

		p.log[s].ID_LIST_PR.ACK(v)
		p.log[s].ID_LIST_PR.ACK(p.ID())

		p.Send(v, Propose{
			Ballot:     p.ballot,
			ID:         p.ID(),
			Request:    *p.log[s].request,
			View:       p.view,
			Slot:       s,
			Command:    p.log[s].command,
			ID_LIST_PR: p.ID(),
		})
		if p.count >= e.ID_LIST_PR.Total()/2{
			break
		}
	}
	log.Debugf("p.ID_LIST_PR.ID : %v", p.log[s].ID_LIST_PR.ID)

}
// handle propose from primary
func (p *Tendermint) handlePropose(m Propose) {
	log.Debugf("\n<-------P-------------handlePropose--------P---------->\n")
	log.Debugf("Sender:%v ", m.ID)
	log.Debugf("(p.slot: %v, m.slot: %v, m.Command.Key: %v) ", p.slot, m.Slot, m.Command.Key)
	log.Debugf("m.ID_LIST_PR.ID = %v", m.ID_LIST_PR)

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
			ID_LIST_PR:	paxi.NewQuorum(),
			ID_LIST_PV: paxi.NewQuorum(),
			ID_LIST_PC: paxi.NewQuorum(),
		}
	}
	e, ok := p.log[m.Slot]

	for i, _ := range p.log[m.Slot].ID_LIST_PR.ID{
		if m.ID == i{
			log.Debugf("Proposed message is already sent")
			return
		}
	}

	e.ID_LIST_PR.ACK(m.ID_LIST_PR)
	e.ID_LIST_PR.ACK(p.ID())
	//log.Debugf("list %v", p.Member.Neibors[i])

	p.Member.Addmember(p.ID())
	p.count = 0
	for i := len(p.Member.Neibors)-1; i >= 0; i-- {
		found := false
		for i2, _ := range p.log[m.Slot].ID_LIST_PR.ID {
			if p.Member.Neibors[i] == i2 {
				found = true
				break
			}
		}
		if !found {
			p.count++
			p.log[m.Slot].ID_LIST_PR.ACK(p.Member.Neibors[i])
			p.Send(p.Member.Neibors[i], Propose{
				Ballot:     p.ballot,
				ID:         p.ID(),
				Request:    m.Request,
				View:       m.View,
				Slot:       m.Slot,
				Command:    m.Command,
				ID_LIST_PR: p.ID(),
			})

		}
		if p.count >= e.ID_LIST_PR.Total()/2 {
			log.Debugf("Only half time sending")
			break
		}

	}
	p.count = 0
	for _, v1 := range p.Member.Neibors {
		value := false
		for i3, _ := range p.log[m.Slot].ID_LIST_PV.ID {
			if i3 == v1 {
				value = true
				break
			}
		}
		if !value {
			p.count++
			p.log[m.Slot].ID_LIST_PV.ACK(v1)
			//p.log[m.Slot].ID_LIST_PV.ACK(p.ID())
			p.Send(v1, PreVote{
				Ballot:     p.ballot,
				ID:         p.ID(),
				Slot:       m.Slot,
				Request:    m.Request,
				Command:    m.Command,
				ID_LIST_PV: p.ID(),
			})
		}
		if p.count >= p.log[m.Slot].ID_LIST_PR.Total()/2 {
			log.Debugf("Only half time sending")
			break
		}
	}
	log.Debugf("p.log[m.Slot].ID_LIST_PR.ID: %v", p.log[m.Slot].ID_LIST_PR.ID )
	log.Debugf("members %v", p.Member.Neibors)
	log.Debugf("p.log[m.Slot].ID_LIST_PV.ID: %v", p.log[m.Slot].ID_LIST_PV.ID )
	log.Debugf("p.log[m.Slot].ID_LIST_PC.ID: %v",p.log[m.Slot].ID_LIST_PC.ID )

	log.Debugf("++++++++++++++++++++++++++ handlePropose Done ++++++++++++++++++++++++++")
}

func (p *Tendermint) HandlePreVote(m PreVote) {
	log.Debugf("\n\n\n<---------V-----------HandlePreVote----------V-------->")
	log.Debugf("Sender ID %v", m.ID)
	log.Debugf("p.slot %v ", p.slot)
	log.Debugf("m.slot %v ", m.Slot)
	log.Debugf("command key  %v ", m.Command.Key)
	log.Debugf("Members  %v ", p.Member.Size())

	if p.slot-p.Member.ClientSize()+1 > m.Slot {
		log.Debugf("Old and Commit slot")
		return
	}

	if m.Ballot > p.ballot {
		log.Debugf("m.ballot is bigger")
		p.ballot = m.Ballot
	}

	_, ok := p.log[m.Slot]
	if !ok {
		log.Debugf("We cannot allocate the log b/c prevote b/f request")
		return
	}

	//	p.log[m.Slot] = &entry{
	//	Ballot:    p.ballot,
	//	commit:    false,
	//	request:   &m.Request,
	//	Timestamp: time.Now(),
	//	command:   m.Command,
	//	ID_LIST_PR: paxi.NewQuorum(),
	//	ID_LIST_PV: paxi.NewQuorum(),
	//	ID_LIST_PC: paxi.NewQuorum(),
	//}
	//}

	//if (p.log[m.Slot].ID_LIST_PR.Size() == 0 ) && (p.log[m.Slot].ID_LIST_PC.Size() == 0 && p.log[m.Slot].ID_LIST_PV.Size() == 0) {
	//	log.Debugf("Old Value")
	//	return
	//}

	log.Debugf("p.log[m.Slot].ID_LIST_PV.Size(): %v", p.log[m.Slot].ID_LIST_PV.Size())
	log.Debugf("p.log[m.Slot].ID_LIST_PR.Size(): %v", p.log[m.Slot].ID_LIST_PR.Size())
	log.Debugf("p.log[m.Slot].ID_LIST_PC.Size(): %v", p.log[m.Slot].ID_LIST_PC.Size())

	for i, _ := range p.log[m.Slot].ID_LIST_PV.ID{
		if p.ID() == i{
			log.Debugf("I already prevote")
			return
		}
	}

	p.count = 0
	p.Member.Addmember(p.ID())

	for _, v1 := range p.Member.Neibors {
		found := false
		for v2, _ := range p.log[m.Slot].ID_LIST_PV.ID {
			if v1 == v2{
				found = true
				break
			}
		}
		if !found {
			p.count++
			p.log[m.Slot].ID_LIST_PV.ACK(m.ID)
			p.log[m.Slot].ID_LIST_PV.ACK(v1)
			p.log[m.Slot].ID_LIST_PV.ACK(p.ID())
			p.Send(v1, PreVote{
				Ballot:     p.ballot,
				ID:         p.ID(),
				Slot:       m.Slot,
				Request:    m.Request,
				Command:    m.Command,
				ID_LIST_PV: p.ID(),
			})
		}
		if p.count >= p.log[m.Slot].ID_LIST_PV.Total()/2 {
			log.Debugf("Only half time sending")
			break
		}
	}

	log.Debugf("p.log[m.Slot].ID_LIST_PC.ID %v", p.log[m.Slot].ID_LIST_PC.ID)
	log.Debugf("p.log[m.Slot].ID_LIST_PV.ID %v", p.log[m.Slot].ID_LIST_PV.ID)

	p.count = 0
	for _, v1 := range p.Member.Neibors {
		value := false
		for v3, _ := range p.log[m.Slot].ID_LIST_PC.ID {
			if v1 == v3{
				value = true
				break
			}
		}
		if !value {
			p.count++
			p.log[m.Slot].ID_LIST_PC.ACK(v1)
			p.log[m.Slot].ID_LIST_PC.ACK(p.ID())
			p.Send(v1, PreCommit{
				Ballot:     p.ballot,
				ID:         p.ID(),
				Slot:       m.Slot,
				Request:    m.Request,
				Command:    m.Command,
				ID_LIST_PC: v1,
				Commit:     false,
			})
		}
		if p.count >= p.log[m.Slot].ID_LIST_PV.Total()/2 {
			log.Debugf("Only half time sending")
			break
		}
	}
}

func (p *Tendermint) HandlePreCommit(m PreCommit) {
	log.Debugf("\n\n<----------C----------HandlePreCommit--------C---------->")

	log.Debugf("Sender ID %v", m.ID)
	log.Debugf("p.slot %v ", p.slot)
	log.Debugf("m.slot %v ", m.Slot)
	log.Debugf(" p.Member.ClientSize() %v ", p.Member.ClientSize())

	if p.slot - p.Member.ClientSize() + 1 > m.Slot{
		log.Debugf("Old and Commit slot")
		return
	}
	if m.Ballot > p.ballot {
		log.Debugf("m is bigger than p")
		p.ballot = m.Ballot
	}
	log.Debugf("command key  %v ", m.Command.Key)
	log.Debugf("Members  %v ", p.Member.Size())

	//if p.Member.Size() == 0{
	//	log.Debugf("Return")
	//	return
	//}

	_, ok := p.log[m.Slot]
	if !ok {
		log.Debugf("We cannot allocate the log b/c precommit is old or early")
		log.Debugf("Old MSG")
		return
	}

	if !p.log[m.Slot].command.Equal(m.Command) && p.log[m.Slot].request == nil {
		log.Debugf("Not consistent")
		return
	}

	if p.Member.Size() <= 2{
		p.Member.Addmember(p.ID())
	}

	for vi, _ := range p.log[m.Slot].ID_LIST_PC.ID {
		found := false
			if vi == m.ID_LIST_PC{
				found = true
				break
			}
		if !found {
			p.log[m.Slot].ID_LIST_PC.ACK(vi)
			break
		}
	}
	if p.log[m.Slot].ID_LIST_PC.Size() == 0{
		p.log[m.Slot].ID_LIST_PC.ACK(m.ID)
	}
	log.Debugf("p.log[m.Slot].ID_LIST_PV %v", p.log[m.Slot].ID_LIST_PV.ID)
	log.Debugf("p.log[m.Slot].ID_LIST_PC %v", p.log[m.Slot].ID_LIST_PC.ID)

	//for i, _ := range p.log[m.Slot].ID_LIST_PC.ID{
	//	if p.ID() == i{
	//		log.Debugf("I already precommit")
	//		return
	//	}
	//}
	p.count = 0
	for _, v1 := range p.Member.Neibors {
		found := false
		for v2, _ := range p.log[m.Slot].ID_LIST_PC.ID {
			if v1 == v2{
				found = true
				break
			}
		}
		if !found {
			p.count++
			p.mux.Lock()
			p.log[m.Slot].ID_LIST_PC.ACK(v1)
			p.mux.Unlock()
			p.Send(v1,PreCommit{
				Ballot:     p.ballot,
				ID:         p.ID(),
				Slot:       m.Slot,
				Request:    m.Request,
				Command:    m.Command,
				ID_LIST_PC: v1,
				Commit:     false,
			})
		}
		if p.count >= p.log[m.Slot].ID_LIST_PC.Total()/2 {
			log.Debugf("Only half time sending")
			break
		}
	}

	log.Debugf("p.log[m.Slot].ID_LIST_PV %v", p.log[m.Slot].ID_LIST_PV.ID)
	log.Debugf("p.log[m.Slot].ID_LIST_PC %v", p.log[m.Slot].ID_LIST_PC.ID)

	if p.log[m.Slot].ID_LIST_PC.Size() >= p.log[m.Slot].ID_LIST_PC.Total() - 1 && p.log[m.Slot].ID_LIST_PV.Size() >= p.log[m.Slot].ID_LIST_PV.Total() - p.log[m.Slot].ID_LIST_PV.Total()/2 || p.log[m.Slot].commit{
		//p.mux.Lock()
		//if p.log[m.Slot].commit == false{
		//p.log[m.Slot].ID_LIST_PC.Reset()
		//p.log[m.Slot].ID_LIST_PV.Reset()
		p.log[m.Slot].commit = true
		//}
		p.log[m.Slot].Ballot = p.ballot
		//p.mux.Unlock()
		p.exec()

	}
}

func (p *Tendermint) Late(slot int, m paxi.Request) {
	log.Debugf("In the function")
	e, ok := p.log[slot]
	if !ok{
		log.Debugf("No thing")
		return
	}
	log.Debugf("p.log[m.Slot].ID_LIST_PR %v", p.log[slot].ID_LIST_PR.ID)
	log.Debugf("p.log[m.Slot].ID_LIST_PV %v", p.log[slot].ID_LIST_PV.ID)
	log.Debugf("p.log[m.Slot].ID_LIST_PC %v", p.log[slot].ID_LIST_PC.ID)
	log.Debugf("e.commit %v", p.log[slot].commit)
	if e.commit == true{
		e.request = &m
		reply := paxi.Reply{
			Command:    m.Command,
		}
		e.request.Reply(reply)
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