package sim

import (
	"fmt"

	"github.com/ANAXMD/ForgeDB/raft"
	"github.com/ANAXMD/ForgeDB/wal"
)

// disk is what the simulator's driver needs from persistent storage.
type disk interface {
	Save(hs *raft.HardState, ents []raft.Entry)
	Sync()
	Crash()
	Load() raft.Persisted
}

var (
	_ disk = (*Disk)(nil)
	_ disk = (*WALDisk)(nil)
)

// WALDisk is a disk backed by the real WAL on an in-memory file (wal.MemFS).
// It has the same contract as Disk: writes are not durable until Sync, and a
// Crash loses everything since the last Sync. Unlike Disk, every restart parses
// the actual bytes with the WAL's recovery code, so the whole simulated cluster
// exercises the real persistence layer.
type WALDisk struct {
	fs *wal.MemFS
	w  *wal.WAL
}

func NewWALDisk() *WALDisk {
	d := &WALDisk{fs: wal.NewMemFS()}
	d.open()
	return d
}

func (d *WALDisk) open() {
	w, err := wal.Open(d.fs)
	if err != nil {
		// A real node would refuse to start; in the simulator that is a failure
		// of the persistence layer and must not be hidden.
		panic(fmt.Sprintf("sim.WALDisk: WAL recovery failed: %v", err))
	}
	d.w = w
}

func (d *WALDisk) Save(hs *raft.HardState, ents []raft.Entry) {
	if err := d.w.Save(hs, ents); err != nil {
		panic(fmt.Sprintf("sim.WALDisk: Save failed: %v", err))
	}
}

func (d *WALDisk) Sync() {
	if err := d.w.Sync(); err != nil {
		panic(fmt.Sprintf("sim.WALDisk: Sync failed: %v", err))
	}
}

// Crash power-cuts the disk: queued and unsynced writes are lost and the WAL
// object is discarded. The next Load reopens the file and runs recovery.
func (d *WALDisk) Crash() {
	d.fs.Crash(0)
	d.w = nil
}

func (d *WALDisk) Load() raft.Persisted {
	if d.w == nil {
		d.open()
	}
	return d.w.Load()
}
