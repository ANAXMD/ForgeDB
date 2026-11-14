// Command trace runs a deterministic Raft simulation and exports its event log.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ANAXMD/ForgeDB/sim"
)

type traceFile struct {
	Seed      int64    `json:"seed"`
	Nodes     int      `json:"nodes"`
	TraceHash uint64   `json:"trace_hash"`
	Events    []string `json:"events"`
}

func main() {
	seed := flag.Int64("seed", 42, "deterministic simulation seed")
	nodes := flag.Int("nodes", 5, "number of Raft nodes (minimum 3)")
	proposals := flag.Int("proposals", 10, "number of client commands to propose")
	duration := flag.Int64("duration", 5000, "additional virtual milliseconds to simulate")
	drop := flag.Float64("drop", 0.05, "message drop probability in [0,1]")
	dup := flag.Float64("dup", 0.02, "message duplication probability in [0,1]")
	output := flag.String("out", "trace.json", "output JSON file")
	flag.Parse()

	if *nodes < 3 || *proposals < 0 || *duration < 0 ||
		*drop < 0 || *drop > 1 || *dup < 0 || *dup > 1 {
		fmt.Fprintln(os.Stderr, "invalid arguments: nodes >= 3, proposals/duration >= 0, drop/dup in [0,1]")
		os.Exit(2)
	}

	s := sim.New(sim.Config{
		Seed: *seed, N: *nodes, DropProb: *drop, DupProb: *dup,
		PreVote: true, CheckQuorum: true,
	})
	s.KeepTrace = true

	if !s.RunUntil(5000, func() bool {
		_, ok := s.Leader()
		return ok
	}) {
		fmt.Fprintln(os.Stderr, "no leader elected within 5000 virtual milliseconds")
		os.Exit(1)
	}

	for i := 0; i < *proposals; i++ {
		accepted := false
		for attempt := 0; attempt < 100 && !accepted; attempt++ {
			leader, ok := s.Leader()
			if ok {
				command := []byte(fmt.Sprintf("command-%d", i+1))
				accepted = s.Propose(leader, command)
			}
			if !accepted {
				s.Run(50)
			}
		}
		if !accepted {
			fmt.Fprintf(os.Stderr, "could not propose command %d after 100 attempts\\n", i+1)
			os.Exit(1)
		}
		s.Run(50)
	}
	s.Run(*duration)

	result := traceFile{
		Seed: *seed, Nodes: *nodes, TraceHash: s.TraceHash(),
		Events: append([]string(nil), s.TraceLog...),
	}
	file, err := os.Create(*output)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		_ = file.Close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := file.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %d events to %s (seed=%d, trace_hash=%d)\n",
		len(result.Events), *output, result.Seed, result.TraceHash)

}
