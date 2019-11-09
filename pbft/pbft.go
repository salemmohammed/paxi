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
	cmd    		  paxi.Command
}
// pbft instance
type Pbft struct{
	paxi.Node

	config 			[]paxi.ID
	N 				paxi.Config
	log      		map[int]*entry 					// log ordered by slot
	activeView  	bool							// current view
	slot     		int            					// highest slot number
	view     		paxi.View 	   					// view number

	ballot   		paxi.Ballot    					// highest ballot number
	execute 		int    							// next execute slot number
	requests 		[]*paxi.Request

	VQ              *paxi.Quorum

	QPrePrepare		func(*paxi.Quorum) bool
	QPrepare        func(*paxi.Quorum) bool
	QCommit       	func(*paxi.Quorum) bool

	QView      	    func(*paxi.Quorum) bool


	Digest 			[]byte
	ReplyWhenCommit bool


	list 			map[paxi.ID]int
	MyCommand       paxi.Command
	MyRequests		*paxi.Request
	TemID			paxi.ID
	Qc3		   	  	*paxi.Quorum
	Qc4		   	  	*paxi.Quorum
	Mprepare       	bool
	seq				int
	seq1			int
	committed      map[paxi.ID]int
	preprepared    map[paxi.ID]int
	prepared       map[paxi.ID]int
	keys		   []paxi.ID
}

// NewPbft creates new pbft instance
func NewPbft(n paxi.Node, options ...func(*Pbft)) *Pbft {
	p := &Pbft{
		Node:            n,
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:            -1,
		activeView:		 false,
		requests:        make([]*paxi.Request, 0),

		VQ:				 paxi.NewQuorum(),

		QPrePrepare:     func(q *paxi.Quorum) bool { return q.Size() == 1},
		QPrepare:        func(q *paxi.Quorum) bool { return q.Majority()},
		QCommit:         func(q *paxi.Quorum) bool { return q.Majority()},

		QView:           func(q *paxi.Quorum) bool { return q.Majority()},

		ReplyWhenCommit: false,
		Digest:          make([]byte,0),
		list:			make(map[paxi.ID]int,0),
		TemID: 			n.ID(),
		Qc3:			paxi.NewQuorum(),
		Qc4:			paxi.NewQuorum(),
		Mprepare: 		false,
		seq:			0,
		seq1:			0,
		committed: 		make(map[paxi.ID]int),
		preprepared: 	make(map[paxi.ID]int),
		prepared:		make(map[paxi.ID]int),
		keys:			make([]paxi.ID, 0),
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

func (p *Pbft) NonPrimaryRequest(r paxi.Request)  {
	p.MyCommand = r.Command
	p.MyRequests = &r
	p.TemID = r.Command.ClientID
}

// HandleRequest handles request and start phase 1
//This is done by the node that client connected to
func (p *Pbft) HandleRequest(r paxi.Request) {

	log.Debugf("<--------------------HandleRequest------------------>")

	log.Debugf("[ p.activeView %t, p.ID() %v ]", p.activeView, p.ID())

	digestValue1 := GetMD5Hash(&r)
	p.requests = append(p.requests, &r)


	log.Debugf("[p.ballot.ID %v, p.ballot %v ]", p.ballot.ID(), p.ballot)

	if p.IsLeader(p.ID()) {
		if p.ballot.ID() != p.ID() {
			p.ballot.Next(p.ID())
		}
		log.Debugf("PrePrepare will be called")
		p.PrePrepare(&r, &digestValue1)
	}
}

// Pre_prepare starts phase 1 PrePrepare
// the primary will send <<pre-prepare,v,n,d(m)>,m>
func (p *Pbft) PrePrepare(r *paxi.Request,s *[]byte) {

	log.Debugf("<--------------------PrePrepare------------------>")
	p.slot++
	log.Debugf("p.slot=%v", p.slot)

	p.log[p.slot] = &entry{
		ballot:    p.ballot,
		view:      p.view,
		command:   r.Command,
		commit:    false,
		request:   r,
		timestamp: time.Now(),
		Digest:    p.Digest,
		Q1:        paxi.NewQuorum(),
		Q2:        paxi.NewQuorum(),
		Q3:        paxi.NewQuorum(),
		Q4:        paxi.NewQuorum(),
		status:    PREPREPARED,
	}

	p.log[p.slot].Q1.ACK(p.ID())

	if p.QPrePrepare(p.log[p.slot].Q1) {
		p.Broadcast(PrePrepare{
			Ballot:     p.ballot,
			ID:         p.ID(),
			View:       p.view,
			Slot:       p.slot,
			Request:    *p.requests[0],
			Digest:     *s,
			ActiveView: p.activeView,
			Command:    r.Command,
		})
	}
	log.Debugf("++++++ PrePrepare Done ++++++")
}

// HandleP1a handles Pre_prepare message
func (p *Pbft) HandlePre(m PrePrepare) {
	log.Debugf("<--------------------HandlePre------------------>")

	log.Debugf(" m.Slot  %v ", m.Slot )
	p.slot = paxi.Max(p.slot, m.Slot)
	log.Debugf("p.slot  %v ", p.slot )

	if m.Ballot > p.ballot {
		log.Debugf("m.Ballot > p.ballot")
		p.ballot = m.Ballot
		p.view = m.View
	}

	// old message
	if m.Ballot < p.ballot {
		log.Debugf("old message")
		return
	}

	log.Debugf("p.activeView=%t", p.activeView)

	p.Digest = GetMD5Hash(&m.Request)
	for i, v := range p.Digest {
		if v != m.Digest[i] {
			log.Debugf("i should be here")
			return
		}
	}

	log.Debugf("m.Ballot=%v , p.ballot=%v, m.view=%v", m.Ballot, p.ballot, m.View)


	p.log[p.slot] = &entry{
		ballot:    	p.ballot,
		view:      	p.view,
		command:   	m.Request.Command,
		commit:    	false,
		request:   	&m.Request,
		timestamp: 	time.Now(),
		Digest:    	m.Digest,
		status:		PREPREPARED,
		Q1:			paxi.NewQuorum(),
		Q2:			paxi.NewQuorum(),
		Q3:			paxi.NewQuorum(),
		Q4:			paxi.NewQuorum(),
	}

	p.log[p.slot].Q2.ACK(p.ID())

	log.Debugf("at the prepare handling")

	if p.QPrePrepare(p.log[p.slot].Q2) {
		p.Broadcast(Prepare{
			Ballot:     p.ballot,
			ID:         p.ID(),
			View:       p.view,
			Slot:       p.slot,
			Digest:     m.Digest,
			Command:    m.Command,
			Request:    m.Request,
		})
	}
	log.Debugf("++++++ HandlePre Done ++++++")
}

// HandlePrepare starts phase 2 HandlePrepare
func (p *Pbft) HandlePrepare(m Prepare) {
	log.Debugf("<--------------------HandlePrepare------------------>")

	log.Debugf("p.slot=%v", p.slot)
	log.Debugf("m.slot=%v", m.Slot)
	//p.slot = paxi.Max(p.slot, m.Slot)

	//id := m.ID
	//s := m.Slot
	e, ok := p.log[m.Slot]
	if !ok || m.Ballot < e.ballot || e.commit ||  p.view != m.View || e.request == nil{
		log.Debugf("************* old message HandlePrepare *************")
		p.prepared[m.ID]++
		log.Debugf("p.PREPARED %v", p.prepared)
		for k,v := range p.prepared {
			log.Debugf("[%v,v%]", k,v)
			if v == 1{
				p.keys = append(p.keys, k)
			}
		}
		p.keys = unique(p.keys)
		log.Debugf("len(p.keys) %v", len(p.keys))
		log.Debugf("p.keys %v",p.keys)
		if(len(p.keys)>1){
			p.slot = paxi.Max(p.slot, m.Slot)
			log.Debugf("HandlePrepare missed")
			p.log[p.slot] = &entry{
				ballot:    	p.ballot,
				view:      	p.view,
				command:   	m.Request.Command,
				commit:    	false,
				request:   	&m.Request,
				timestamp: 	time.Now(),
				status:		PREPREPARED,
				Q1:			paxi.NewQuorum(),
				Q2:			paxi.NewQuorum(),
				Q3:			paxi.NewQuorum(),
				Q4:			paxi.NewQuorum(),
			}
			p.log[p.slot].status = PREPARED
			p.Broadcast(Commit{
				Ballot:  p.ballot,
				ID:      p.ID(),
				View:    p.view,
				Slot:    m.Slot,
				//Digest:  m.Digest,
				Command: m.Command,
				Request: m.Request,
			})
		}
		return
	}
	e.Q3.ACK(m.ID)
	for _,v := range p.keys{
		e.Q3.ACK(v)
	}
	//p.slot = paxi.Max(p.slot, m.Slot)
	// old message
	for i, v := range p.Digest {
			if v != m.Digest[i] {
				log.Debugf("digest message")
				return
			}
	}
	if e.status == PREPREPARED {
		log.Debugf("status HandlePrepare %v", e.status)
		if p.QPrepare(e.Q3) {
			e.Q3.Reset()
			e.status = PREPARED
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

	log.Debugf("m.slot=%v", m.Slot)
	//p.slot = paxi.Max(p.slot, m.Slot)
	e, exist := p.log[m.Slot]
	if exist{
		e.Q4.ACK(m.ID)
		if e.status != PREPARED{
			p.seq++
			log.Debugf("status HandleCommit %v, p.seq %v ", e.status, p.seq)
		}
		if e.request != nil && e.status == PREPARED{
			p.seq1++
			log.Debugf("status HandleCommit %v, p.seq1 %v ", e.status, p.seq1)
		}
		if p.QCommit(e.Q4) && e.status == PREPARED{
			e.Q4.Reset()
			e.commit = true
			e.status = COMMITTED
		}else{
			if p.seq >2{
				log.Debugf("p.seq majority")
				e.commit = true
				e.status = COMMITTED
			}else{
			log.Debugf("not Enough majority")
			return
			}
		}
	}else{
		log.Debugf("else HandleCommit")
		p.log[m.Slot] = &entry{
			Q4:paxi.NewQuorum(),
		}
		e = p.log[m.Slot]
		p.Qc4.ACK(m.ID)
		if p.QCommit(p.Qc4){
			p.Qc4.Reset()
			e.commit = true
		}
	}

	e.ballot = m.Ballot
	e.view = m.View
	e.command = m.Command


	// old message
	if m.Ballot < p.ballot && p.view != m.View {
		log.Debugf("old msg in commit")
		return
	}
	//digestValue3 := GetMD5Hash(&m.Request)
	for i, v := range p.Digest {
		if v != m.Digest[i] {
			return
		}
	}
	if p.ReplyWhenCommit && e.request != nil {
		e.request.Reply(paxi.Reply{
			Command:   e.request.Command,
			Timestamp: e.request.Timestamp,
		})
	} else {
		log.Debugf("we are in exec reset slot %v, m.slot %v", p.slot, m.Slot)
		p.exec()
	}
	log.Debugf("********* Commit End *********** ")
	//p.Q3.Reset()
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

		if e.request != nil && p.ID()==p.view.ID(){
			log.Debugf(" ********* Primary Request ********* \n %v",*e.request)
			reply := paxi.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			e.request.Reply(reply)
			log.Debugf("********* Reply Primary *********")
			e.request = nil
			p.seq1=0
			p.seq=0
		}

		log.Debugf("TemID=%v",p.TemID)

		if p.MyRequests != nil && e.request != nil && p.ID()==p.TemID && p.TemID!=p.view.ID(){
			log.Debugf("********* Replica Request ********* \n %v",p.MyRequests)
			log.Debugf("p.ID() =%v",p.ID())
			reply := paxi.Reply{
				Command:    p.MyCommand,
				Value:      value,
				Properties: make(map[string]string),
			}
			p.MyRequests.Reply(reply)
			log.Debugf("********* Reply Replicas *********")
			p.MyRequests = nil
			p.seq1=0
			p.seq=0
		}
		// TODO clean up the log periodically
		delete(p.log, p.execute)
		p.execute++
	}
}
func unique(intSlice []paxi.ID) []paxi.ID {
	keys := make(map[paxi.ID]bool)
	list := []paxi.ID{}
	for _, entry := range intSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}