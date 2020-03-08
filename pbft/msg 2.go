package pbft

import (
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi"
)

func init() {
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
	return fmt.Sprintf("PrePrepare {Ballot=%v , View=%v, slot=%v, Command=%v}", m.Ballot,m.View,m.Slot,m.Command)
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
	return fmt.Sprintf("Prepare {Ballot=%v, ID=%v, View=%v, slot=%v, command=%v}", m.Ballot,m.ID,m.View,m.Slot,m.Command)
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
}

func (m Commit) String() string {
	return fmt.Sprintf("Commit {Ballot=%v, ID=%v, View=%v, Slot=%v, command=%v}", m.Ballot,m.ID,m.View,m.Slot, m.Command)
}