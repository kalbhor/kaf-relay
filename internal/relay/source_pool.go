package relay

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type SourcePoolCfg struct {
	HealthCheckInterval time.Duration
	ReqTimeout          time.Duration
	LagThreshold        int64
	MaxRetries          int
	EnableBackoff       bool
	BackoffMin          time.Duration
	BackoffMax          time.Duration
}

// Server represents a source Server's config with health and weight
// parameters which are used for tracking health status.
type Server struct {
	Config ConsumerCfg
	ID     int

	// Weight is the cumulative high watermark (offset) of every single topic
	// on a source. This is used for comparing lags between different sources
	// based on a threshold. If a server is unhealthy, the weight is marked as -1.
	Weight map[string]int64

	Healthy bool

	// This is only set when a new live Kafka consumer connection is established
	// on demand via Get(), where a server{} is returned. Internally, no connections
	// are maintained on SourcePool.[]servers and only the config, weight etc.
	// params are used to keep track of healthy servers.
	Client *kgo.Client
}

// TopicOffsets defines topic->partition->offset map for any src/target kafka cluster
type TopicOffsets map[string]map[int32]kgo.Offset

// SourcePool manages the source Kafka instances and consumption.
type SourcePool struct {
	cfg         SourcePoolCfg
	client      *kgo.Client
	log         *slog.Logger
	metrics     *metrics.Set
	targetToSrc map[string]string
	srcToTarget map[string]string

	srcTopics []string

	// targetOffsets is initialized with current topic high watermarks from target.
	// These are updated whenever new msgs from src are sent to target for producing.
	// Whenever a new direct src consumer starts consuming from respective topic it uses
	// the offsets from this map. (These happen independently in the pool loop, hence no lock)
	targetOffsets TopicOffsets

	// List of all source servers.
	servers []Server

	// The server amongst all the given one's that is best placed to be the current
	// "healthiest". This is determined by weights (that track offset lag) and status
	// down. It's possible that curCandidate can itself be down or unhealthy,
	// for instance, when all sources are down. In such a scenario, the poll simply
	// keeps retriying until a real viable candidate is available.
	curCandidate map[string]Server

	fetchCtx    context.Context
	fetchCancel context.CancelFunc

	backoffFn func(int) time.Duration
	sync.Mutex
}

const (
	unhealthyWeight int64 = -1
)

var (
	ErrorNoHealthy = errors.New("no healthy node")
)

// NewSourcePool returns a controller instance that manages the lifecycle of a pool of N source (consumer)
// servers. The pool always attempts to find one healthy node for the relay to consume from.
func NewSourcePool(cfg SourcePoolCfg, serverCfgs []ConsumerCfg, topics Topics, targetOffsets TopicOffsets, m *metrics.Set, l *slog.Logger) (*SourcePool, error) {
	// Initially mark all servers as unhealthy.
	weights := make(map[string]int64)
	for topic := range topics {
		weights[topic] = unhealthyWeight
	}
	servers := make([]Server, 0, len(serverCfgs))
	for n, c := range serverCfgs {
		servers = append(servers, Server{
			ID:      n,
			Weight:  weights,
			Healthy: false,
			Config:  c,
		})
	}

	var (
		targToSrc = make(map[string]string, len(topics))
		srcToTarg = make(map[string]string, len(topics))
		srcTopics = make([]string, 0, len(topics))
	)
	for src, targ := range topics {
		srcTopics = append(srcTopics, src)
		targToSrc[targ.TargetTopic] = src
		srcToTarg[src] = targ.TargetTopic
	}

	log.Printf("target2src: %+v", targToSrc)
	sp := &SourcePool{
		cfg:         cfg,
		targetToSrc: targToSrc,
		srcToTarget: srcToTarg,
		srcTopics:   srcTopics,
		servers:     servers,
		log:         l,
		metrics:     m,
		backoffFn:   getBackoffFn(cfg.EnableBackoff, cfg.BackoffMin, cfg.BackoffMax),
	}

	sp.setInitialOffsets(targetOffsets)
	return sp, nil
}

// setInitialOffsets sets the offset/weight from the target on boot so that the messages
// can be consumed from the offsets where they were left off.
func (sp *SourcePool) setInitialOffsets(of TopicOffsets) {
	// Assign the current weight as initial target offset.
	// This is done to resume if target already has messages published from src.
	weights := make(map[string]int64)
	for topic, p := range of {
		var w int64
		for _, o := range p {
			w += o.EpochOffset().Offset
		}
		weights[topic] = w
	}

	currCandidate := make(map[string]Server)
	for topic := range of {
		currCandidate[topic] = Server{
			Healthy: false,
			Weight:  weights,
		}
	}

	sp.targetOffsets = of

	// Set the current candidate with initial weight and a placeholder ID. This initial
	// weight ensures we resume consuming from where last left off. A real
	// healthy node should replace this via background checks
	sp.log.Debug("setting initial target node weight", "weight", weights, "topics", of)
	sp.curCandidate = currCandidate
}

// Get attempts return a healthy source Kafka client connection.
// It internally applies backoff/retries between connection attempts and thus can take
// indefinitely long to return based on the config.
func (sp *SourcePool) Get(globalCtx context.Context, topic string) (*Server, error) {
	retries := 0
loop:
	for {
		select {
		case <-globalCtx.Done():
			return nil, globalCtx.Err()
		default:
			if sp.cfg.MaxRetries != IndefiniteRetry && retries >= sp.cfg.MaxRetries {
				return nil, fmt.Errorf("`max_retries`(%d) exhausted; exiting relay", sp.cfg.MaxRetries)
			}

			// Get the config for a healthy node.
			s, err := sp.getCurCandidate(topic)
			if err == nil {
				sp.log.Debug("attempting new source connection", "id", s.ID, "broker", s.Config.BootstrapBrokers, "retries", retries)
				conn, err := sp.newConn(globalCtx, s, topic)
				if err != nil {
					retries++
					sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcNetworkErrMetric, s.ID, "new connection failed")).Inc()
					sp.log.Error("new source connection failed", "id", s.ID, "broker", s.Config.BootstrapBrokers, "error", err, "retries", retries)
					waitTries(globalCtx, sp.backoffFn(retries))
					continue loop
				}

				// XXX: Cache the current live connection internally.
				sp.client = conn

				out := s
				out.Client = conn

				sp.fetchCtx, sp.fetchCancel = context.WithCancel(globalCtx)
				return &out, nil
			}

			retries++
			sp.metrics.GetOrCreateCounter(SrcsUnhealthyMetric).Inc()
			sp.log.Error("no healthy server found. waiting and retrying", "retries", retries, "error", err)
			waitTries(globalCtx, sp.backoffFn(retries))
		}
	}
}

// GetFetches retrieves a Kafka fetch iterator to retrieve individual messages from.
func (sp *SourcePool) GetFetches(s *Server) (kgo.Fetches, error) {
	sp.log.Debug("retrieving fetches from source", "id", s.ID, "broker", s.Config.BootstrapBrokers)
	fetches := s.Client.PollFetches(sp.fetchCtx)

	// There's no connection.
	if fetches.IsClientClosed() {
		sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcKafkaErrMetric, s.ID, "client closed")).Inc()
		sp.log.Debug("retrieving fetches failed. client closed.", "id", s.ID, "broker", s.Config.BootstrapBrokers)
		sp.setWeight(s.ID, unhealthyWeight)

		return nil, errors.New("fetch failed")
	}

	// If there are errors in the fetches, handle them.
	for _, err := range fetches.Errors() {
		sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcKafkaErrMetric, s.ID, "fetches error")).Inc()
		sp.log.Error("found error in fetches", "server", s.ID, "error", err.Err)
		sp.setWeight(s.ID, unhealthyWeight)

		return nil, errors.New("fetch failed")
	}

	return fetches, nil
}

// RecordOffsets records the offsets of the latest fetched records per topic.
// This is used to resume consumption on new connections/reconnections from the source during runtime.
func (sp *SourcePool) RecordOffsets(rec *kgo.Record) error {
	if sp.targetOffsets == nil {
		sp.targetOffsets = make(TopicOffsets)
	}

	srcTopic, ok := sp.srcToTarget[rec.Topic]
	if !ok {
		return fmt.Errorf("could not find target topic for src topic %s", rec.Topic)
	}

	if o, ok := sp.targetOffsets[srcTopic]; ok {
		// If the topic already exists, update the offset for the partition.
		o[rec.Partition] = kgo.NewOffset().At(rec.Offset + 1)
		sp.targetOffsets[srcTopic] = o
	} else {
		// If the topic does not exist, create a new map for the topic.
		o := make(map[int32]kgo.Offset)
		o[rec.Partition] = kgo.NewOffset().At(rec.Offset + 1)
		sp.targetOffsets[srcTopic] = o
	}

	return nil
}

func (sp *SourcePool) GetHighWatermark(ctx context.Context, cl *kgo.Client) (kadm.ListedOffsets, error) {
	return getHighWatermark(ctx, cl, sp.srcTopics, sp.cfg.ReqTimeout)
}

// Close closes the active source Kafka client.
func (sp *SourcePool) Close() {
	if sp.client != nil {
		// Prevent blocking on close.
		sp.client.PurgeTopicsFromConsuming()
	}
}

// newConn initializes a new consumer group config.
func (sp *SourcePool) newConn(ctx context.Context, s Server, topic string) (*kgo.Client, error) {
	sp.log.Debug("running TCP health check", "id", s.ID, "server", s.Config.BootstrapBrokers, "session_timeout", s.Config.SessionTimeout)
	if ok := checkTCP(ctx, s.Config.BootstrapBrokers, s.Config.SessionTimeout); !ok {
		return nil, ErrorNoHealthy
	}

	sp.log.Debug("initiazing new source consumer", "id", s.ID, "server", s.Config.BootstrapBrokers)
	cl, err := sp.initConsumer(s.Config, topic)
	if err != nil {
		sp.log.Error("error initiazing source consumer", "id", s.ID, "server", s.Config.BootstrapBrokers)
		return nil, err
	}

	return cl, nil
}

// healthcheck indefinitely monitors the health of source (consumer) nodes and keeps the status
// and weightage of the nodes updated.
func (sp *SourcePool) healthcheck(ctx context.Context, signal map[string]chan struct{}) error {
	tick := time.NewTicker(sp.cfg.HealthCheckInterval)
	defer tick.Stop()

	// Copy the servers to be used in the infinite loop below so that the main list
	// of servers don't have to be locked.
	sp.Lock()
	servers := make([]Server, 0, len(sp.servers))
	servers = append(servers, sp.servers...)
	sp.Unlock()

	clients := make([]*kgo.Client, len(servers))
	for {
		select {
		case <-ctx.Done():
			sp.log.Debug("ending healthcheck goroutine")

			// Close all open admin Kafka clients.
			for _, cl := range clients {
				if cl != nil {
					cl.Close()
				}
			}

			return ctx.Err()

		case <-tick.C:
			// Fetch offset counts for each server.
			wg := &sync.WaitGroup{}

			curServerWeight := make(map[string]int64)
			for _, topic := range sp.srcTopics {
				curServerWeight[topic] = unhealthyWeight
			}

			for i, s := range servers {
				sp.log.Debug("running background health check", "id", s.ID, "server", s.Config.BootstrapBrokers)

				// For the first ever check, clients will be nil.
				if clients[i] == nil {
					sp.log.Debug("initializing admin client for background check", "id", s.ID, "server", s.Config.BootstrapBrokers)
					cl, err := sp.initConsumerClient(s.Config)
					if err != nil {
						sp.log.Error("error initializing admin client in background healthcheck", "id", s.ID, "server", s.Config.BootstrapBrokers, "error", err)
						continue
					}

					sp.log.Debug("initialized admin client for background check", "id", s.ID, "server", s.Config.BootstrapBrokers)
					clients[i] = cl
				}

				// Spawn a goroutine for the client to concurrently fetch its offsets. The waitgroup
				// ensures that offsets for all servers are fetched and then tallied together for healthcheck.
				wg.Add(1)
				go func(idx int, s Server) {
					defer wg.Done()

					// Get the highest offset of all the topics on the source server and sum them up
					// to derive the weight of the server.
					sp.log.Debug("getting high watermark via admin client for background check", "id", idx)
					offsets, err := sp.GetHighWatermark(ctx, clients[idx])
					if err != nil && offsets == nil {
						sp.log.Error("error fetching offset in background healthcheck", "id", s.ID, "server", s.Config.BootstrapBrokers, "error", err)
						sp.setWeight(servers[idx].ID, unhealthyWeight)
						return
					}

					offsets.Each(func(lo kadm.ListedOffset) {
						servers[idx].Weight[lo.Topic] = lo.Offset
						sp.setWeight(servers[idx].ID, lo.Offset, lo.Topic)

						// Adjust the global health of the servers.
						if servers[idx].ID == sp.curCandidate[lo.Topic].ID {
							curServerWeight[lo.Topic] = lo.Offset
						}
					})

				}(i, s)
			}
			wg.Wait()

			// Now that offsets/weights for all servers are fetched, check if the current server
			// is lagging beyond the threshold.
			for _, s := range servers {
				for topic, curr := range sp.curCandidate {
					if curr.ID == s.ID {
						continue
					}

					lag := s.Weight[topic] - curServerWeight[topic]
					if lag > sp.cfg.LagThreshold {
						sp.log.Error("current server's lag threshold exceeded. Marking as unhealthy.", "id", s.ID, "server", s.Config.BootstrapBrokers, "diff", lag, "threshold", sp.cfg.LagThreshold)
						sp.setWeight(s.ID, unhealthyWeight)

						// Cancel any active fetches.
						sp.fetchCancel()

						// Signal the relay poll loop to start asking for a healthy client.
						// The push is non-blocking to avoid getting stuck trying to send on the poll loop
						// if the poll loop's subsection (checking for errors) has already sent a signal
						select {
						case signal[topic] <- struct{}{}:
						default:
						}
					}
				}
			}
		}
	}
}

var offsetPool = sync.Pool{
	New: func() interface{} {
		return make(TopicOffsets)
	},
}

// initConsumer initializes a Kafka consumer client. This is used for creating consumer connection to source servers.
func (sp *SourcePool) initConsumer(cfg ConsumerCfg, topic string) (*kgo.Client, error) {
	srcOffsets := offsetPool.Get().(TopicOffsets)
	defer func() {
		for k := range srcOffsets {
			delete(srcOffsets, k)
		}
		offsetPool.Put(srcOffsets)
	}()

	targ, ok := sp.srcToTarget[topic]
	if !ok {
		return nil, fmt.Errorf("target topic not found for src consumer on %s", topic)
	}

	// For each target topic get the relevant src topic to configure direct
	// consumer to start from target topic's last offset.
	for t, of := range sp.targetOffsets {
		if t == targ {
			srcOffsets[topic] = of
			break
		}
	}

	opts := []kgo.Opt{
		kgo.ConsumePartitions(srcOffsets),
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(sp.cfg.ReqTimeout),
	}

	if cfg.EnableLog {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)))
	}

	if cfg.EnableAuth {
		opts = addSASLConfig(opts, cfg.KafkaCfg)
	}

	if cfg.EnableTLS {
		if cfg.CACertPath == "" && cfg.ClientCertPath == "" && cfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := getTLSConfig(cfg.CACertPath, cfg.ClientCertPath, cfg.ClientKeyPath)
			if err != nil {
				return nil, err
			}

			// Set up TLS configuration.
			opts = append(opts, tlsOpt)
		}
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	if err := testConnection(cl, cfg.SessionTimeout, sp.srcTopics, nil); err != nil {
		return nil, err
	}

	return cl, nil
}

// initConsumerClient returns franz-go client with default config.
func (sp *SourcePool) initConsumerClient(cfg ConsumerCfg) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(sp.cfg.ReqTimeout),
		kgo.SessionTimeout(cfg.SessionTimeout),
	}

	if cfg.EnableLog {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)))
	}

	if cfg.EnableAuth {
		opts = addSASLConfig(opts, cfg.KafkaCfg)
	}

	if cfg.EnableTLS {
		if cfg.CACertPath == "" && cfg.ClientCertPath == "" && cfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := getTLSConfig(cfg.CACertPath, cfg.ClientCertPath, cfg.ClientKeyPath)
			if err != nil {
				return nil, err
			}

			// Set up TLS configuration
			opts = append(opts, tlsOpt)
		}
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	return client, err
}

// getCurCandidate returns the most viable candidate server config (highest weight and not down).
// If everything is down, it returns the one with the highest weight.
func (sp *SourcePool) getCurCandidate(topic string) (Server, error) {
	sp.Lock()
	defer sp.Unlock()

	curr := sp.curCandidate[topic]

	// If the weight (sum of all high watermarks of all topics on the source) is -1,
	// the server is unhealthy.
	if curr.Weight[topic] == unhealthyWeight || !curr.Healthy {
		return curr, ErrorNoHealthy
	}

	return curr, nil
}

// setWeight updates the weight (cumulative offset highwatermark for all topics on the server)
// for a particular server. If it's set to -1, the server is assumed to be unhealthy.
func (sp *SourcePool) setWeight(id int, weight int64, topics ...string) {
	sp.Lock()
	defer sp.Unlock()

	for _, s := range sp.servers {
		if s.ID != id {
			continue
		}

		for _, topic := range topics {
			s.Weight[topic] = weight
			if weight != unhealthyWeight {
				s.Healthy = true
			}

			// If the incoming server's weight is greater than the current candidate,
			// promote that to the current candidate.
			if weight > sp.curCandidate[topic].Weight[topic] {
				sp.curCandidate[topic] = s
			}
		}

		sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcHealthMetric, id)).Set(uint64(weight))
		sp.log.Debug("setting candidate weight", "id", id, "weight", weight, "curr", sp.curCandidate)
		sp.servers[id] = s
		break
	}
}
