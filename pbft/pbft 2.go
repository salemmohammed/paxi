package pbft

import (
	"crypto/md5"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"time"
)
type status int8
const (
	NONE status = iota
	PREPREPARED
	PREPARED
	COMMITTED
)
// log's entries
type entry struct {
	ballot     	  paxi.Ballot
	view	   	  paxi.View
	command    	  paxi.Command
	commit     	  bool
	request    	  *paxi.Request
	timestamp  	  time.Time
	Digest     	  []byte
	Q1		   	  *paxi.Quorum
	Q2		   	  *paxi.Quorum
	Q3		   	  *paxi.Quorum
	Q4		   	  *paxi.Quorum
	status		  status
}
// helping log
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
	MissReq		 *paxi.Request
}

// pbft instance
type Pbft struct{
	paxi.Node

	config 			[]paxi.ID
	N 				paxi.Config
	log      		map[int]*entry 					// log ordered by slot
	logR      		map[int]*RequestSlot 		    // log ordered by slot for receiving requests
	activeView  	bool							// current view
	slot     		int            					// highest slot number
	view     		paxi.View 	   					// view number
	ballot   		paxi.Ballot    					// highest ballot number
	execute 		int    							// next execute slot number
	requests 		[]*paxi.Request
	quorum   		*paxi.Quorum    				// phase 1 quorum
	ReplyWhenCommit bool
	RecivedReq		bool
}

// NewPbft creates new pbft instance
func NewPbft(n paxi.Node, options ...func(*Pbft)) *Pbft {
	p := &Pbft{
		Node:            n,
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		logR:            make(map[int]*RequestSlot, paxi.GetConfig().BufferSize),
		quorum:        	 paxi.NewQuorum(),
		slot:            -1,
		activeView:		 false,
		requests:        make([]*paxi.Request, 0),
		ReplyWhenCommit: false,
		RecivedReq:false,
	}
	for _, opt := range options {
		opt(p)
	}
	return p
}
// IsLeader indicates if this node is current leader
func (p *Pbft) IsLeader(id paxi.ID) bool {
	return p.activeView && p.view.ID() == p.ID()
}
// Digest message
func GetMD5Hash(r *paxi.Request) []byte  {
	hasher := md5.New()
	hasher.Write([]byte(r.Command.Value))
	return []byte(hasher.Sum(nil))
}
// HandleRequest handles request and start phase 1
//This is done by the node that client connected to
func (p *Pbft) HandleRequest(r paxi.Request, s int) {
	log.Debugf("<--------------------HandleRequest------------------>")

	e, ok := p.log[s]
	if !ok{
		log.Debugf("create a log")
		p.log[s] = &entry{
			ballot:    p.ballot,
			view:      p.view,
			command:   r.Command,
			commit:    false,
			request:   &r,
			timestamp: time.Now(),
			Digest:    GetMD5Hash(&r),
			Q1:        paxi.NewQuorum(),
			Q2:        paxi.NewQuorum(),
			Q3:        paxi.NewQuorum(),
			Q4:        paxi.NewQuorum(),
		}
	}
	e, ok = p.log[s]

	e.ballot  	= p.ballot
	e.view	  	= p.view
	e.command 	= r.Command
	e.commit  	= false
	e.request 	= &r
	e.timestamp = time.Now()
	e.Digest	= GetMD5Hash(&r)
	e.Q1		= paxi.NewQuorum()
	e.Q2		= paxi.NewQuorum()
	e.Q3		= paxi.NewQuorum()
	e.Q4		= paxi.NewQuorum()


	e.Digest = GetMD5Hash(&r)
	e.Q1.ACK(p.ID())
	log.Debugf("[p.ballot.ID %v, p.ballot %v ]", p.ballot.ID(), p.ballot)

	if p.activeView{
		log.Debugf("PrePrepare will be called")
		p.PrePrepare(&r, &e.Digest, s)
	}
}
// Pre_prepare starts phase 1 PrePrepare
// the primary will send <<pre-prepare,v,n,d(m)>,m>
func (p *Pbft) PrePrepare(r *paxi.Request,s *[]byte, slt int) {
	log.Debugf("<--------------------PrePrepare------------------>")

	p.Broadcast(PrePrepare{
			Ballot:     p.ballot,
			ID:         p.ID(),
			View:       p.view,
			Slot:       slt,
			Request:    *r,
			Digest:     *s,
			ActiveView: p.activeView,
			Command:    r.Command,
		})
	log.Debugf("++++++ PrePrepare Done ++++++")
}
// HandleP1a handles Pre_prepare message
func (p *Pbft) HandlePre(m PrePrepare) {
	log.Debugf("<--------------------HandlePre------------------>")

	log.Debugf(" Sender  %v ", m.ID )

	log.Debugf(" m.Slot  %v ", m.Slot )

	if m.Ballot > p.ballot {
		log.Debugf("m.Ballot > p.ballot")
		p.ballot = m.Ballot
		p.view = m.View
	}

	e, ok := p.log[m.Slot]
	if !ok{
		log.Debugf("Create a log")
		p.log[m.Slot] = &entry{
			ballot:    	p.ballot,
			view:      	p.view,
			command:   	m.Command,
			commit:    	false,
			request:   	&m.Request,
			timestamp: 	time.Now(),
			Digest:    	m.Digest,
			Q1:			paxi.NewQuorum(),
			Q2:			paxi.NewQuorum(),
			Q3:			paxi.NewQuorum(),
			Q4:			paxi.NewQuorum(),
		}
	}
	e, ok = p.log[m.Slot]
	e.Q2.ACK(m.ID)
	e.Q2.ACK(p.ID())
	// old message
	if m.Ballot < p.ballot {
		log.Debugf("old message")
		return
	}
	log.Debugf("p.activeView: %v", p.activeView)
	e.Digest = GetMD5Hash(&m.Request)
	for i, v := range e.Digest {
		if v != m.Digest[i] {
			log.Debugf("i should be here")
			return
		}
	}
	log.Debugf("m.Ballot=%v , p.ballot=%v, m.view=%v", m.Ballot, p.ballot, m.View)
	log.Debugf("at the prepare handling")
	p.Broadcast(Prepare{
			Ballot:     p.ballot,
			ID:         p.ID(),
			View:       m.View,
			Slot:       m.Slot,
			Digest:     m.Digest,
			Command:    m.Command,
			Request:    m.Request,
		})
	log.Debugf("++++++ HandlePre Done ++++++")
}
// HandlePrepare starts phase 2 HandlePrepare
func (p *Pbft) HandlePrepare(m Prepare) {
	log.Debugf("<--------------------HandlePrepare------------------>")
	log.Debugf(" Sender  %v ", m.ID )
	log.Debugf("p.slot=%v", p.slot)
	log.Debugf("m.slot=%v", m.Slot)

	e, ok := p.log[m.Slot]

	if !ok || m.Ballot < e.ballot ||  p.view != m.View || e.request == nil{
		log.Debugf("we create a log")
		p.log[m.Slot] = &entry{
			ballot:    	p.ballot,
			view:      	p.view,
			command:   	m.Command,
			commit:    	false,
			request:   	&m.Request,
			timestamp: 	time.Now(),
			Digest:    	m.Digest,
			Q1:			paxi.NewQuorum(),
			Q2:			paxi.NewQuorum(),
			Q3:			paxi.NewQuorum(),
			Q4:			paxi.NewQuorum(),
		}
	}
	e, ok = p.log[m.Slot]
	e.Q3.ACK(m.ID)

	// old message
	e.Digest = GetMD5Hash(&m.Request)
	for i, v := range e.Digest {
			if v != m.Digest[i] {
				log.Debugf("digest message")
				return
			}
	}
	if e.Q3.Majority() || e.Q4.Majority() {
		log.Debugf("My status :%v", e.status )
		if e.status != COMMITTED {
			e.status = COMMITTED
			//e.status = PREPARED
			e.Q3.Reset()
			p.Broadcast(Commit{
				Ballot:  p.ballot,
				ID:      p.ID(),
				View:    p.view,
				Slot:    m.Slot,
				Digest:  m.Digest,
				Command: m.Command,
				Request: m.Request,
			})
		}
	}
	log.Debugf("++++++ HandlePrepare Done ++++++")
}

// HandleCommit starts phase 3
func (p *Pbft) HandleCommit(m Commit) {
	log.Debugf("<--------------------HandleCommit------------------>")
	log.Debugf(" Sender  %v ", m.ID)
	log.Debugf("m.slot=%v", m.Slot)
	log.Debugf("p.slot=%v", p.slot)

	e, exist := p.log[m.Slot]

	if !exist {
		log.Debugf("create a log")
		p.log[m.Slot] = &entry{
			ballot:    p.ballot,
			view:      p.view,
			command:   m.Command,
			commit:    false,
			request:   &m.Request,
			timestamp: time.Now(),
			Digest:    m.Digest,
			Q1:        paxi.NewQuorum(),
			Q2:        paxi.NewQuorum(),
			Q3:        paxi.NewQuorum(),
			Q4:        paxi.NewQuorum(),
		}
	}
	e, exist = p.log[m.Slot]
	e.Q3.ACK(m.ID)

	e.Digest = GetMD5Hash(&m.Request)
	for i, v := range e.Digest {
		if v != m.Digest[i] {
			log.Debugf("digest message")
			return
		}
	}
	e.Q4.ACK(m.ID)
	if e.Q4.Majority() && e.commit != true {
		if e.status != COMMITTED {
			e.status = COMMITTED
			log.Debugf("We need to send prepare message")
			p.Broadcast(Commit{
				Ballot:  p.ballot,
				ID:      p.ID(),
				View:    p.view,
				Slot:    m.Slot,
				Digest:  m.Digest,
				Command: m.Command,
				Request: m.Request,
			})
		}
		e.Q4.Reset()
		e.commit = true
		e.ballot = m.Ballot
		e.view = m.View
		e.command = m.Command
	}
	// old message
	if m.Ballot < p.ballot && p.view != m.View {
		log.Debugf("old msg in commit")
		return
	}
	if p.ReplyWhenCommit && e.request != nil {
		e.request.Reply(paxi.Reply{
			Command:   e.request.Command,
			Timestamp: e.request.Timestamp,
		})
	}

	e1, ok1 := p.logR[m.Slot]
	if !ok1 {
	log.Debugf("The logR did not create or already deleted")
		return
	}

	log.Debugf("we are in exec reset p.slot %v, m.slot %v", p.slot, m.Slot)
	if e.commit == true && e1.active == true{
		p.exec()
	}
	log.Debugf("********* Commit End *********** ")
}

func (p *Pbft) exec() {
	log.Debugf("<--------------------exec()------------------>")
	for {
		log.Debugf("p.execute %v", p.execute)
		e, ok := p.log[p.execute]
		if !ok{
			return
		}
		if !ok || !e.commit {
			break
		}
		value := p.Execute(e.command)
		log.Debugf("value=%v",value)

		if e.request != nil && p.activeView{
			log.Debugf(" ********* Primary Request ********* %v",*e.request)
			reply := paxi.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			e.request.Reply(reply)
			log.Debugf("********* Reply Primary *********")
			e.request = nil
		}

		e1, ok1 := p.logR[p.execute]
		if !ok1 {
			log.Debugf("NULL")
			return
		}

		if e.request != nil && !e1.Leader {
			log.Debugf("********* Replica Request ********* ")
			log.Debugf("p.ID() =%v", p.ID())
			reply := paxi.Reply{
				Command:    p.logR[p.execute].request.Command,
				Value:      value,
				Properties: make(map[string]string),
			}
			p.logR[p.execute].request.Reply(reply)
			p.logR[p.execute].request = nil
			log.Debugf("********* Reply Replicas *********")
		}
		// TODO clean up the log periodically
		delete(p.log, p.execute)
		delete(p.logR, p.execute)
		p.execute++
	}
}