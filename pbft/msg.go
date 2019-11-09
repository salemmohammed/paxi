package pbft

import (
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(ViewChange{})
	gob.Register(LeaderChange{})
	gob.Register(PrePrepare{})
	gob.Register(Prepare{})
	gob.Register(Commit{})
}

// <PrePrepare,seq,v,s,d(m),m>
type PrePrepare struct {
	Ballot 		paxi.Ballot
	ID     		paxi.ID
	View 		paxi.View
	Slot    	int
	Request 	paxi.Request
	Digest 		[]byte
	ActiveView	bool
	Command 	paxi.Command
}

func (m PrePrepare) String() string {
	return fmt.Sprintf("PrePrepare {b=%v v=%v s=%v r=%v c=%v id=%v}", m.Ballot,m.View,m.Slot,m.Request,m.Command,m.ID)
}

// Prepare message
type Prepare struct {
	Ballot 	paxi.Ballot
	ID     	paxi.ID
	View   	paxi.View
	Slot   	int
	Digest 	[]byte
	Command paxi.Command
	Request paxi.Request
}

func (m Prepare) String() string {
	return fmt.Sprintf("Prepare {b=%v id=%v v=%v s=%v c=%v}", m.Ballot,m.ID,m.View,m.Slot,m.Command)
}

// Commit  message
type Commit struct {
	Ballot   paxi.Ballot
	ID    	 paxi.ID
	View 	 paxi.View
	Slot     int
	Digest 	 []byte
	Command  paxi.Command
	Request  paxi.Request
	Mprepare bool
}

func (m Commit) String() string {
	return fmt.Sprintf("Commit {b=%v id=%v v=%v s=%v c=%v, Mprepare%v}", m.Ballot,m.ID,m.View,m.Slot, m.Command, m.Mprepare)
}

// ViewChange selects/discovers who is the leader
type ViewChange struct {
	View    paxi.View
	ID 		paxi.ID
	Request paxi.Request
}

func (l ViewChange) String() string {
	return fmt.Sprintf("ViewChange {View=%v, ID=%v, Request=%v}", l.View, l.ID,l.Request)
}

// LeaderChange tells others who is a new leader
type LeaderChange struct {
	View   paxi.View
	ID 	   paxi.ID
}

func (l LeaderChange) String() string {
	return fmt.Sprintf("LeaderChange {View=%v, ID=%v}", l.View,l.ID)
}