package kafkatarget

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zerodha/kaf-relay/pkg/relay"
)

func TestWritePartition(t *testing.T) {
	cases := []struct {
		name            string
		partition       int32
		sourcePartition int32
		want            int32
	}{
		{"mirrors source partition", relay.SourcePartition, 2, 2},
		{"uses explicit partition", 1, 2, 1},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target := &Target{inletCh: make(chan *kgo.Record, 1)}
			if err := target.Write(context.Background(), relay.Message{
				Topic: "target", Partition: c.partition, SourcePartition: c.sourcePartition,
			}); err != nil {
				t.Fatal(err)
			}

			if got := (<-target.inletCh).Partition; got != c.want {
				t.Errorf("record partition = %d, want %d", got, c.want)
			}
		})
	}
}
