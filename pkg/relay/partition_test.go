package relay

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

type recordingTarget struct {
	messages []Message
}

func (*recordingTarget) GetHighWatermark(context.Context) (Offsets, error) { return nil, nil }
func (*recordingTarget) Start() error                                      { return nil }
func (*recordingTarget) Close() error                                      { return nil }
func (t *recordingTarget) Write(_ context.Context, msg Message) error {
	t.messages = append(t.messages, msg)
	return nil
}

func TestRelayPreservesSourcePartitions(t *testing.T) {
	target := &recordingTarget{}
	r, err := NewRelay(RelayCfg{}, nil, target, Topic{
		SourceTopic: "source", TargetTopic: "target", AutoTargetPartition: true,
	}, nil, metrics.NewSet(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	for _, partition := range []int32{0, 1, 2} {
		if err := r.processMessage(context.Background(), &kgo.Record{
			Topic: "source", Partition: partition, Value: []byte("value"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(target.messages) != 3 {
		t.Fatalf("target received %d messages, want 3", len(target.messages))
	}

	for i, msg := range target.messages {
		want := int32(i)
		if msg.Partition != SourcePartition {
			t.Errorf("message %d target partition = %d, want SourcePartition", i, msg.Partition)
		}
		if msg.SourcePartition != want {
			t.Errorf("message %d source partition = %d, want %d", i, msg.SourcePartition, want)
		}
	}
}

func TestRelayExplicitTargetPartition(t *testing.T) {
	target := &recordingTarget{}
	r, err := NewRelay(RelayCfg{}, nil, target, Topic{
		SourceTopic: "source", TargetTopic: "target", TargetPartition: 2,
	}, nil, metrics.NewSet(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	for _, partition := range []int32{0, 1, 2} {
		if err := r.processMessage(context.Background(), &kgo.Record{
			Topic: "source", Partition: partition, Value: []byte("value"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(target.messages) != 3 {
		t.Fatalf("target received %d messages, want 3", len(target.messages))
	}

	for i, msg := range target.messages {
		if msg.Partition != 2 {
			t.Errorf("source partition %d sent to target partition %d, want 2", i, msg.Partition)
		}
		if msg.SourcePartition != int32(i) {
			t.Errorf("message %d source partition = %d, want %d", i, msg.SourcePartition, i)
		}
	}
}
