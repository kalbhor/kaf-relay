package relay

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// memTarget is a non-Kafka Target that stores messages in memory and tracks the
// next offset to consume per source partition, like a Redis or DB target would.
type memTarget struct {
	topic string

	mu      sync.Mutex
	offsets TopicOffsets
	msgs    []string
}

func newMemTarget(topic string, offsets TopicOffsets) *memTarget {
	if offsets == nil {
		offsets = TopicOffsets{}
	}
	return &memTarget{topic: topic, offsets: offsets}
}

func (m *memTarget) GetHighWatermark(context.Context) (Offsets, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make(TopicOffsets, len(m.offsets))
	for p, o := range m.offsets {
		out[p] = o
	}
	return Offsets{m.topic: out}, nil
}

func (m *memTarget) Start() error { return nil }
func (m *memTarget) Close() error { return nil }

func (m *memTarget) Write(_ context.Context, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.msgs = append(m.msgs, string(msg.Value))
	m.offsets[msg.SourcePartition] = msg.Offset + 1
	return nil
}

func (m *memTarget) received() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := append([]string(nil), m.msgs...)
	sort.Strings(out)
	return out
}

// newSource starts an in-memory Kafka cluster with a topic of the given partitions.
func newSource(t *testing.T, topic string, partitions int32) *kfake.Cluster {
	t.Helper()

	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// produce writes one message per value to the given source partition.
func produce(t *testing.T, c *kfake.Cluster, topic string, partition int32, values ...string) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, v := range values {
		rec := &kgo.Record{Topic: topic, Partition: partition, Value: []byte(v)}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// runRelay relays from the source into the target the way library users wire it: the
// target's high watermark is the resume point. It stops once the target holds want
// messages (or on timeout) and a short settle period has passed to catch duplicates.
func runRelay(t *testing.T, c *kfake.Cluster, topic string, target *memTarget, want int) {
	t.Helper()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.NewSet()
	tp := Topic{SourceTopic: topic, TargetTopic: topic, AutoTargetPartition: true}

	hw, err := target.GetHighWatermark(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	pool, err := NewSourcePool(SourcePoolCfg{
		HealthCheckInterval: 100 * time.Millisecond,
		ReqTimeout:          time.Second,
		LagThreshold:        100,
		MaxRetries:          -1,
	}, []ConsumerCfg{{KafkaCfg: KafkaCfg{BootstrapBrokers: c.ListenAddrs(), SessionTimeout: time.Second}}}, tp, hw[topic], m, log)
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRelay(RelayCfg{}, pool, target, tp, nil, m, log)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for len(target.received()) < want && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func values(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return out
}

func assertReceived(t *testing.T, target *memTarget, want ...[]string) {
	t.Helper()

	var all []string
	for _, w := range want {
		all = append(all, w...)
	}
	sort.Strings(all)

	got := target.received()
	if fmt.Sprint(got) != fmt.Sprint(all) {
		t.Fatalf("target received %d messages %v, want %d %v", len(got), got, len(all), all)
	}
}

func TestMemTargetFreshStart(t *testing.T) {
	const topic = "orders"
	c := newSource(t, topic, 3)

	p0, p1, p2 := values("p0", 3), values("p1", 2), values("p2", 4)
	produce(t, c, topic, 0, p0...)
	produce(t, c, topic, 1, p1...)
	produce(t, c, topic, 2, p2...)

	target := newMemTarget(topic, nil)
	runRelay(t, c, topic, target, 9)
	assertReceived(t, target, p0, p1, p2)
}

func TestMemTargetResume(t *testing.T) {
	const topic = "orders"
	c := newSource(t, topic, 3)

	first := values("first", 3)
	produce(t, c, topic, 0, first...)

	target := newMemTarget(topic, nil)
	runRelay(t, c, topic, target, 3)
	assertReceived(t, target, first)

	// Restart with the offsets the target stored. Partition 0 resumes, and partitions
	// the target has never written to must still be consumed.
	p0, p1 := values("p0", 2), values("p1", 2)
	produce(t, c, topic, 0, p0...)
	produce(t, c, topic, 1, p1...)

	runRelay(t, c, topic, target, 7)
	assertReceived(t, target, first, p0, p1)
}
