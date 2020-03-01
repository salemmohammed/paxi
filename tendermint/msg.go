package tendermint

import (
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi"
)
func init() {
	gob.Register(Propose{})
	gob.Register(PreVote{})
	gob.Register(PreCommit{})
}
// Propose
type Propose struct {
	Ballot 		paxi.Ballot
	ID     		paxi.ID
	Request 	paxi.Request
	Slot 		int
	Command 	paxi.Command
	View		paxi.View
	ID_LIST_PR  paxi.ID
	Active      bool
}
func (m Propose) String() string {
	return fmt.Sprintf("Propose {Ballot %v,Command %v, Slot %v, ID_LIST_PR %v, Active %v}", m.Ballot, m.Command, m.Slot, m.ID_LIST_PR, m.Active)
}
// PreVote
type PreVote struct {
	Ballot 	paxi.Ballot
	ID     	paxi.ID
	Request paxi.Request
	Slot 	int
	Command paxi.Command
	View	paxi.View
	ID_LIST_PV  paxi.ID
	Active      bool
}
func (m PreVote) String() string {
	return fmt.Sprintf("PreVote {Ballot %v,ID %v,Command %v, Slot %v, View %v, ID_LIST_PV %v}", m.Ballot, m.ID, m.Command, m.Slot, m.View, m.ID_LIST_PV)
}
// PreCommit  message
type PreCommit struct {
	Ballot 	paxi.Ballot
	ID     	paxi.ID
	Request paxi.Request
	PreVote paxi.Quorum
	Command paxi.Command
	Slot 	int
	Commit  bool
	ID_LIST_PC  paxi.ID
	Active      bool
}
func (m PreCommit) String() string {
	return fmt.Sprintf("PreCommit {Ballot %v, Command %v, Commit %v, Slot %v, ID_LIST_PC %v}", m.Ballot,  m.Command, m.Commit,m.Slot, m.ID_LIST_PC)
}