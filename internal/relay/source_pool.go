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

	// Weights maps the high watermark (offset) of every relevant topic
	// on a source. This is used for comparing lags between different sources
	// based on a threshold. If a server is unhealthy, the weight is marked as -1.
	Weights map[string]int64

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
	cfg SourcePoolCfg
	//client      *kgo.Client
	log         *slog.Logger
	metrics     *metrics.Set
	targetToSrc map[string]string
	srcTopics   []string

	// targetOffsets is initialized with current topic high watermarks from target.
	// These are updated whenever new msgs from src are sent to target for producing.
	// Whenever a new direct src consumer starts consuming from respective topic it uses
	// the offsets from this map. (These happen independently in the pool loop, hence no lock)
	targetOffsets TopicOffsets

	// List of all source servers.
	servers []Server

	// The server amongst all the given one's that is best placed to be the current
	// "healthiest" for a particular topic.
	// This is determined by weights (that track offset lag) and status
	// down. It's possible that curCandidate can itself be down or unhealthy,
	// for instance, when all sources are down. In such a scenario, the poll simply
	// keeps retriying until a real viable candidate is available.
	curCandidate map[string]candidate

	fetchCtx    context.Context
	fetchCancel context.CancelFunc

	backoffFn func(int) time.Duration
	sync.Mutex
}

type candidate struct {
	topic  string
	weight int64
	idx    int
}

const (
	unhealthyWeight int64 = -1
)

var (
	ErrorNoHealthy = errors.New("no healthy node")
)

// NewSourcePool returns a controller instance that manages the lifecycle of a pool of N source (consumer)
// servers. The pool always attempts to find one healthy node for the relay to consume from.
func NewSourcePool(cfg SourcePoolCfg, serverCfgs []ConsumerCfg, topics Topics, targetOffsets TopicOffsets, m *metrics.Set, log *slog.Logger) (*SourcePool, error) {
	servers := make([]Server, 0, len(serverCfgs))

	// Initially mark all servers as unhealthy.
	// XXX
	for n, c := range serverCfgs {
		weights := make(map[string]int64, len(topics))
		for src := range topics {
			weights[src] = unhealthyWeight
		}
		servers = append(servers, Server{
			ID:      n,
			Weights: weights,
			Healthy: false,
			Config:  c,
		})
	}

	var (
		targToSrc = make(map[string]string, len(topics))
		srcTopics = make([]string, 0, len(topics))
	)
	for src, targ := range topics {
		srcTopics = append(srcTopics, src)
		targToSrc[targ.TargetTopic] = src
	}

	sp := &SourcePool{
		cfg:         cfg,
		targetToSrc: targToSrc,
		srcTopics:   srcTopics,
		servers:     servers,
		log:         log,
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
	if sp.curCandidate == nil {
		sp.curCandidate = make(map[string]candidate)
	}
	for t, p := range of {
		var w int64
		for _, o := range p {
			w += o.EpochOffset().Offset
		}
		// Set the current candidate with initial weight and a placeholder ID. This initial
		// weight ensures we resume consuming from where last left off. A real
		// healthy node should replace this via background checks
		sp.log.Debug("setting initial target node weight", "weight", w, "topics", t)
		srcTopic := sp.targetToSrc[t]
		sp.curCandidate[srcTopic] = candidate{
			topic: srcTopic,
			//idx:    -1,
			weight: w,
		}
	}

	sp.targetOffsets = of
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
				conn, err := sp.newConn(globalCtx, s)
				if err != nil {
					retries++
					sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcNetworkErrMetric, s.ID, "new connection failed")).Inc()
					sp.log.Error("new source connection failed", "id", s.ID, "broker", s.Config.BootstrapBrokers, "error", err, "retries", retries)
					waitTries(globalCtx, sp.backoffFn(retries))
					continue loop
				}

				// XXX: Cache the current live connection internally.
				//sp.client = conn

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
		sp.markUnhealthy(s.ID)
		//sp.setWeight(s.ID, unhealthyWeight)

		return nil, errors.New("fetch failed")
	}

	// If there are errors in the fetches, handle them.
	for _, err := range fetches.Errors() {
		sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcKafkaErrMetric, s.ID, "fetches error")).Inc()
		sp.log.Error("found error in fetches", "server", s.ID, "error", err.Err)
		sp.setWeight(s.ID, err.Topic, unhealthyWeight)

		return nil, errors.New("fetch failed")
	}

	return fetches, nil
}

// RecordOffsets records the offsets of the latest fetched records per topic.
// This is used to resume consumption on new connections/reconnections from the source during runtime.
func (sp *SourcePool) RecordOffsets(rec *kgo.Record) {
	if sp.targetOffsets == nil {
		sp.targetOffsets = make(TopicOffsets)
	}

	topic := sp.sr

	if o, ok := sp.targetOffsets[rec.Topic]; ok {
		// If the topic already exists, update the offset for the partition.
		o[rec.Partition] = kgo.NewOffset().At(rec.Offset + 1)
		sp.targetOffsets[rec.Topic] = o
	} else {
		// If the topic does not exist, create a new map for the topic.
		o := make(map[int32]kgo.Offset)
		o[rec.Partition] = kgo.NewOffset().At(rec.Offset + 1)
		sp.targetOffsets[rec.Topic] = o
	}
}

func (sp *SourcePool) GetHighWatermark(ctx context.Context, cl *kgo.Client) (kadm.ListedOffsets, error) {
	return getHighWatermark(ctx, cl, sp.srcTopics, sp.cfg.ReqTimeout)
}

// // Close closes the active source Kafka client.
// func (sp *SourcePool) Close() {
// 	if sp.client != nil {
// 		// Prevent blocking on close.
// 		sp.client.PurgeTopicsFromConsuming()
// 	}
// }

// newConn initializes a new consumer group config.
func (sp *SourcePool) newConn(ctx context.Context, s Server) (*kgo.Client, error) {
	sp.log.Debug("running TCP health check", "id", s.ID, "server", s.Config.BootstrapBrokers, "session_timeout", s.Config.SessionTimeout)
	if ok := checkTCP(ctx, s.Config.BootstrapBrokers, s.Config.SessionTimeout); !ok {
		return nil, ErrorNoHealthy
	}

	sp.log.Debug("initiazing new source consumer", "id", s.ID, "server", s.Config.BootstrapBrokers)
	cl, err := sp.initConsumer(s.Config)
	if err != nil {
		sp.log.Error("error initiazing source consumer", "id", s.ID, "server", s.Config.BootstrapBrokers)
		return nil, err
	}

	return cl, nil
}

// healthcheck indefinitely monitors the health of source (consumer) nodes and keeps the status
// and weightage of the nodes updated.
func (sp *SourcePool) healthcheck(ctx context.Context, signals map[string]chan struct{}) error {
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

			curServerWeight := unhealthyWeight
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

					id := servers[idx].ID

					// Get the highest offset of all the topics on the source server and sum them up
					// to derive the weight of the server.
					sp.log.Debug("getting high watermark via admin client for background check", "id", idx)
					offsets, err := sp.GetHighWatermark(ctx, clients[idx])
					if err != nil && offsets == nil {
						sp.log.Error("error fetching offset in background healthcheck", "id", s.ID, "server", s.Config.BootstrapBrokers, "error", err)
						sp.markUnhealthy(id)
						return
					}

					weights := make(map[string]int64) // TODO: re-use
					offsets.Each(func(lo kadm.ListedOffset) {
						weights[lo.Topic] += lo.Offset
						//sp.setWeight(id, lo.Topic, w)
					})

					for t, w := range weights {
						sp.setWeight(id, t, w)
						curr := sp.curCandidate[t]
						if servers[idx].ID == curr.idx {
							curr.weight = w
							sp.curCandidate[t] = curr
						}
					}

					// NOTE: Check concurrency.
					servers[idx].Weights = weights

				}(i, s)
			}
			wg.Wait()

			// Now that offsets/weights for all servers are fetched, check if the current server for topics
			// is lagging beyond the threshold.
			for topic, curr := range sp.curCandidate {
				for _, s := range servers {
					if curr.idx == s.ID {
						continue
					}

					lag := s.Weights[topic] - curServerWeight

					if lag > sp.cfg.LagThreshold {
						sig := signals[topic]
						sp.log.Error("current server's lag threshold exceeded. Marking as unhealthy.", "id", s.ID, "server", s.Config.BootstrapBrokers, "lag", lag, "threshold", sp.cfg.LagThreshold)
						//sp.setWeight(s.ID, unhealthyWeight)
						sp.markUnhealthy(s.ID)

						// Cancel any active fetches.
						sp.fetchCancel()

						// Signal the relay poll loop to start asking for a healthy client.
						// The push is non-blocking to avoid getting stuck trying to send on the poll loop
						// if the poll loop's subsection (checking for errors) has already sent a signal
						select {
						case sig <- struct{}{}:
						default:
						}
					}
				}
			}
		}
	}
}

// var offsetPool = sync.Pool{
// 	New: func() interface{} {
// 		return make(TopicOffsets)
// 	},
// }

// initConsumer initializes a Kafka consumer client. This is used for creating consumer connection to source servers.
func (sp *SourcePool) initConsumer(cfg ConsumerCfg) (*kgo.Client, error) {
	// TODO: check for mem usage
	srcOffsets := make(TopicOffsets)
	// defer func() {
	// 	for k := range srcOffsets {
	// 		delete(srcOffsets, k)
	// 	}
	// 	offsetPool.Put(srcOffsets)
	// }()

	// For each target topic get the relevant src topic to configure direct
	// consumer to start from target topic's last offset.
	for t, of := range sp.targetOffsets {
		src, ok := sp.targetToSrc[t]
		if !ok {
			return nil, fmt.Errorf("src topic not found for target %s in map", t)
		}
		srcOffsets[src] = of
	}

	log.Printf("source offsets: %+v", srcOffsets)
	log.Printf("consumer-cfg: %+v", cfg)
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

	// If the weight (sum of all high watermarks of all topics on the source) is -1,
	// the server is unhealthy.
	curr := sp.curCandidate[topic]
	currServer := sp.servers[curr.idx]
	if currServer.Weights[topic] == unhealthyWeight || !currServer.Healthy {
		return currServer, ErrorNoHealthy
	}

	return currServer, nil
}

// setWeight updates the weight (cumulative offset highwatermark for all topics on the server)
// for a particular server. If it's set to -1, the server is assumed to be unhealthy.
func (sp *SourcePool) setWeight(id int, topic string, weight int64) {
	sp.Lock()
	defer sp.Unlock()

	for _, s := range sp.servers {
		if s.ID != id {
			continue
		}

		s.Weights[topic] = weight
		if s.Weights[topic] != unhealthyWeight {
			s.Healthy = true
		}

		// If the incoming server's weight is greater than the current candidate,
		// promote that to the current candidate.
		curr := sp.curCandidate[topic]
		if weight > sp.servers[curr.idx].Weights[topic] {
			sp.curCandidate[topic] = candidate{
				idx:    id,
				topic:  topic,
				weight: weight,
			}
		}

		sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcHealthMetric, id)).Set(uint64(weight))
		sp.log.Debug("setting candidate weight", "id", id, "weight", weight, "curr", sp.curCandidate)
		sp.servers[id] = s
		break
	}
}

// setWeight updates the weight (cumulative offset highwatermark for all topics on the server)
// for a particular server. If it's set to -1, the server is assumed to be unhealthy.
func (sp *SourcePool) markUnhealthy(id int) {
	sp.Lock()
	defer sp.Unlock()

	for _, s := range sp.servers {
		if s.ID != id {
			continue
		}

		for t := range s.Weights {
			s.Weights[t] = unhealthyWeight
			//s.Healthy = false
		}

		break
	}
}
