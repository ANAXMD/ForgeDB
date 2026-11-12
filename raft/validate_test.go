package raft

import "testing"

func validTestConfig() Config {
	return Config{
		ID:             1,
		Peers:          []NodeID{1, 2, 3},
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		Rand:           fixedTestRand{},
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"zero node ID", func(c *Config) { c.ID = 0 }},
		{"nil random source", func(c *Config) { c.Rand = nil }},
		{"zero election ticks", func(c *Config) { c.ElectionTicks = 0 }},
		{"negative election ticks", func(c *Config) { c.ElectionTicks = -1 }},
		{"zero heartbeat ticks", func(c *Config) { c.HeartbeatTicks = 0 }},
		{"negative heartbeat ticks", func(c *Config) { c.HeartbeatTicks = -1 }},
		{"heartbeat not less than election", func(c *Config) { c.HeartbeatTicks = 10 }},
		{"empty peers", func(c *Config) { c.Peers = nil }},
		{"zero peer ID", func(c *Config) { c.Peers = []NodeID{1, 0} }},
		{"duplicate peer ID", func(c *Config) { c.Peers = []NodeID{1, 2, 2} }},
		{"own ID missing", func(c *Config) { c.Peers = []NodeID{2, 3} }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			tt.change(&cfg)

			defer func() {
				if recover() == nil {
					t.Fatal("New did not panic for invalid configuration")
				}
			}()

			New(cfg, Persisted{})
		})
	}
}

func TestNewRejectsInvalidPersistedLog(t *testing.T) {
	tests := []struct {
		name string
		log  []Entry
		term Term
	}{
		{
			name: "non-consecutive indexes",
			log: []Entry{
				{Index: 1, Term: 1},
				{Index: 3, Term: 1},
			},
			term: 1,
		},
		{
			name: "decreasing entry terms",
			log: []Entry{
				{Index: 1, Term: 2},
				{Index: 2, Term: 1},
			},
			term: 2,
		},
		{
			name: "entry term above hard state term",
			log: []Entry{
				{Index: 1, Term: 2},
			},
			term: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			p := Persisted{
				HardState: HardState{Term: tt.term},
				Log:       tt.log,
			}

			defer func() {
				if recover() == nil {
					t.Fatal("New did not panic for invalid persisted state")
				}
			}()

			New(cfg, p)
		})
	}
}
