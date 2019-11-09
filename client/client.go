package main

import (
	"encoding/binary"
	"flag"
	"github.com/ailidani/paxi/log"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/chain"
	"github.com/ailidani/paxi/paxos"
)

var id = flag.String("id", "", "node id this client connects to")
var algorithm = flag.String("algorithm", "pbft", "Client API type [paxos, chain]")
var load = flag.Bool("load", false, "Load K keys into DB")
var master = flag.String("master", "", "Master address.")

// db implements Paxi.DB interface for benchmarking
type db struct {
	paxi.Client
}

func (d *db) Init() error {
	return nil
}

func (d *db) Stop() error {
	return nil
}


func (d *db) Read(k int) (value []int) {
	//log.Debugf("Read")
	key := paxi.Key(k)
	v, _ := d.GetMUL(key)
	//log.Debugf("after the Read in client")
	if len(v) == 0 {
		//log.Debugf("len(v) %v", len(v))
		return nil
	}
	var m []int
	for _,v := range v{
		x, _ := binary.Uvarint(v)
		//log.Debugf("x %v", x)
		m = append(m,int(x))
	}
	//log.Debugf("m %v", m)
	return m
}

func (d *db) Write(k, v int) []error {
	key := paxi.Key(k)
	value := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(value, uint64(v))
	err := d.PutMUL(key, value)
	return err
}

func main() {
	paxi.Init()

	if *master != "" {
		paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	}

	d := new(db)
	switch *algorithm {
	case "paxos":
		d.Client = paxos.NewClient(paxi.ID(*id))
	case "chain":
		d.Client = chain.NewClient()
	case "pbft":
		d.Client = paxi.NewHTTPClient(paxi.ID(*id))
	default:
		d.Client = paxi.NewHTTPClient(paxi.ID(*id))
	}

	b := paxi.NewBenchmark(d)
	if *load {
		log.Debugf("Load in Clinet is started")
		b.Load()
	} else {
		log.Debugf("Run in Clinet is started")
		b.Run()
	}
}
