package paxi

import (
	"github.com/ailidani/paxi/log"
	"math"
	"strconv"
	"strings"
)

// Quorum records each acknowledgement and check for different types of quorum satisfied
type Quorum struct {
	size  int
	acks  map[ID]bool
	zones map[int]int
	nacks map[ID]bool
	ID    map[ID]int
	n	  float64
	AID   []ID

}

// NewQuorum returns a new Quorum
func NewQuorum() *Quorum {
	q := &Quorum{
		size:  0,
		acks:  make(map[ID]bool),
		zones: make(map[int]int),
		ID:	   make(map[ID]int),
		AID:   make([]ID,0),
		n:	   -1,
	}
	return q
}

// ACK adds id to quorum ack records
func (q *Quorum) ACK(id ID) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.zones[id.Zone()]++
		q.ID[id]++
		q.AID_ID(id)
	}
	//log.Debugf("q.size %v ",q.size)
}

// NACK adds id to quorum nack records
func (q *Quorum) NACK(id ID) {
	if !q.nacks[id] {
		q.nacks[id] = true
	}
}

// ADD increase ack size by one
func (q *Quorum) ADD() {
	q.size++
}
// ADD increase ack size by one
func (q *Quorum) AID_ID(id ID) {
	for _,v := range q.AID{
		if v == id {
			return
		}
	}
	 q.AID = append(q.AID, id)
	log.Debugf("ids %v", q.AID)
}

// ID increase ack size by one
func (q *Quorum) IDs()int {
	var cont []int
	for i,_ := range q.ID {
		var s string = string(i)
		var b string
		if strings.Contains(s, ".") {
			split := strings.SplitN(s, ".", 2)
			b = split[1]
		} else {
			b = s
		}
		i, _ := strconv.Atoi(b)
		cont = append(cont, i)
	}
	min := 1
	var x float64
	var y int
	for _, v := range cont {
		x = math.Min(float64(min), float64(v))
		y = int(x)
	}
	return y
}

// Size returns current ack size
func (q *Quorum) Size() int {
	return q.size
}

// Reset resets the quorum to empty
func (q *Quorum) Reset() {
	q.size = 0
	q.acks = make(map[ID]bool)
	q.zones = make(map[int]int)
	q.nacks = make(map[ID]bool)
	q.ID = make(map[ID]int)
}

func (q *Quorum) All() bool {
	return q.size == config.n
}

// Majority quorum satisfied
func (q *Quorum) Majority() bool {
	//log.Debugf("Majority()")
	//log.Debugf("q.size=%v",q.size)
	log.Debugf("[q.size >= config.n*2/3] %t", q.size >= ((config.n*2/3)))
	return q.size >= (config.n*2/3)
}

func (q *Quorum) MajorityT() bool {
	//log.Debugf("Majority()")
	//log.Debugf("q.size=%v",q.size)
	log.Debugf("[q.size >= config.n / 2 ] %t",  q.size >= config.n / 2 )
	return q.size >= config.n / 2
}


// Majority quorum satisfied
func (q *Quorum) Total() int {
	return config.n
}

func (q *Quorum) INC() float64{
	q.n++
	return q.n
}

// FastQuorum from fast paxos
func (q *Quorum) FastQuorum() bool {
	return q.size >= config.n*3/4
}

// AllZones returns true if there is at one ack from each zone
func (q *Quorum) AllZones() bool {
	return len(q.zones) == config.z
}

// ZoneMajority returns true if majority quorum satisfied in any zone
func (q *Quorum) ZoneMajority() bool {
	for z, n := range q.zones {
		if n > config.npz[z]/2 {
			return true
		}
	}
	return false
}

// GridRow == AllZones
func (q *Quorum) GridRow() bool {
	return q.AllZones()
}

// GridColumn == all nodes in one zone
func (q *Quorum) GridColumn() bool {
	for z, n := range q.zones {
		if n == config.npz[z] {
			return true
		}
	}
	return false
}

// FGridQ1 is flexible grid quorum for phase 1
func (q *Quorum) FGridQ1(Fz int) bool {
	zone := 0
	for z, n := range q.zones {
		if n > config.npz[z]/2 {
			zone++
		}
	}
	return zone >= config.z-Fz
}

// FGridQ2 is flexible grid quorum for phase 2
func (q *Quorum) FGridQ2(Fz int) bool {
	zone := 0
	for z, n := range q.zones {
		if n > config.npz[z]/2 {
			zone++
		}
	}
	return zone >= Fz+1
}

/*
// Q1 returns true if config.Quorum type is satisfied
func (q *Quorum) Q1() bool {
	switch config.Quorum {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridRow()
	case "fgrid":
		return q.FGridQ1()
	case "group":
		return q.ZoneMajority()
	case "count":
		return q.size >= config.n-config.F
	default:
		log.Error("Unknown quorum type")
		return false
	}
}

// Q2 returns true if config.Quorum type is satisfied
func (q *Quorum) Q2() bool {
	switch config.Quorum {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridColumn()
	case "fgrid":
		return q.FGridQ2()
	case "group":
		return q.ZoneMajority()
	case "count":
		return q.size > config.F
	default:
		log.Error("Unknown quorum type")
		return false
	}
}
*/
