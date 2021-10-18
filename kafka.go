package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

const chanBuffSize = 1000

type pauseTask struct {
	pause bool
	tp    kafka.TopicPartition
}

type PauseEvent struct {
	tp    kafka.TopicPartition
	pause bool
}

type subscribeTask struct {
	subscribe bool
	done      chan struct{}
}

func (p PauseEvent) String() string {
	if p.pause {
		return fmt.Sprintf("paused: %v", p.tp)
	} else {
		return fmt.Sprintf("unpaused: %v", p.tp)
	}
}

type Producer struct {
	input  chan *kafka.Message
	events chan kafka.Event
	config *kafka.ConfigMap
}

func NewProducer(config *kafka.ConfigMap) *Producer {
	p := &Producer{
		input:  make(chan *kafka.Message, chanBuffSize),
		events: make(chan kafka.Event, chanBuffSize),
		config: config,
	}

	return p
}

func (p *Producer) Run(ctx context.Context) {
	defer close(p.events)

	for {
		var producer *kafka.Producer
		var err error

		for producer, err = kafka.NewProducer(p.config); err != nil; producer, err = kafka.NewProducer(p.config) {
			p.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
			timeout := time.After(time.Second)
		loop:
			for {
				select {
				case <-timeout:
					break loop
				case <-ctx.Done():
					return
				case msg := <-p.input:
					msg.TopicPartition.Error = err
					p.events <- msg
				}
			}
		}

		wg := sync.WaitGroup{}

		fatalErrCh := make(chan struct{})

		wg.Add(1)
		go func() {
			defer wg.Done()
			for evt := range producer.Events() {
				if stats, ok := evt.(*kafka.Stats); ok {
					if err := handleStatsEvt(stats); err != nil {
						p.events <- kafka.NewError(kafka.ErrApplication, fmt.Sprintf("can't parse statistics: %v", err), false)
					}
				}
				p.events <- evt
				if errEvt, ok := evt.(kafka.Error); ok && errEvt.IsFatal() {
					select {
					case <-fatalErrCh:
					default:
						close(fatalErrCh)
					}
				}
			}
		}()

	loop2:
		for {
			select {
			case msg := <-p.input:
				producer.ProduceChannel() <- msg
			case <-ctx.Done():
				break loop2
			case <-fatalErrCh:
				break loop2
			}
		}

		producer.Flush(1000)
		producer.Close()

		wg.Wait()

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (p *Producer) Input() chan<- *kafka.Message {
	return p.input
}

func (p *Producer) Events() <-chan kafka.Event {
	return p.events
}

type Consumer struct {
	events          chan kafka.Event
	config          *kafka.ConfigMap
	topics          []string
	rebalanceCb     kafka.RebalanceCb
	pauseTaskCh     chan pauseTask
	subscribeTaskCh chan subscribeTask
	subscribed      bool
}

func NewConsumer(config *kafka.ConfigMap, topics []string, rebalanceCb kafka.RebalanceCb) *Consumer {
	return &Consumer{
		events:          make(chan kafka.Event, chanBuffSize),
		config:          config,
		topics:          topics,
		rebalanceCb:     rebalanceCb,
		pauseTaskCh:     make(chan pauseTask, chanBuffSize),
		subscribeTaskCh: make(chan subscribeTask, chanBuffSize),
		subscribed:      false,
	}
}

func (c *Consumer) RunWithoutSubscribing(ctx context.Context) {
	c.run(ctx, false)
}

func (c *Consumer) Run(ctx context.Context) {
	c.run(ctx, true)
}

func (c *Consumer) run(ctx context.Context, needSubscribing bool) {
	defer close(c.events)

	for {
		var consumer *kafka.Consumer
		var err error
		for consumer, err = kafka.NewConsumer(c.config); err != nil; consumer, err = kafka.NewConsumer(c.config) {
			c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
		loop1:
			for {
				select {
				case <-time.After(time.Second):
					break loop1
				case <-ctx.Done():
					return
				}
			}
		}

		if needSubscribing || c.subscribed {
			for err = consumer.SubscribeTopics(c.topics, c.rebalanceCb); err != nil; err = consumer.SubscribeTopics(c.topics, c.rebalanceCb) {
				c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
			loop2:
				for {
					select {
					case <-time.After(time.Second):
						break loop2
					case <-ctx.Done():
						return
					}
				}
			}

			c.subscribed = true
		}

	loop:
		for {
			select {
			case <-ctx.Done():
				if err = consumer.Close(); err != nil {
					c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
				}
				return
			case pauseTask := <-c.pauseTaskCh:
				if pauseTask.pause {
					if err = consumer.Pause([]kafka.TopicPartition{pauseTask.tp}); err != nil {
						c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
					} else {
						c.events <- PauseEvent{
							tp:    pauseTask.tp,
							pause: true,
						}
					}
				} else {
					if err = consumer.Resume([]kafka.TopicPartition{pauseTask.tp}); err != nil {
						c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
					} else {
						c.events <- PauseEvent{
							tp:    pauseTask.tp,
							pause: false,
						}
					}
				}
			case subscribeTask := <-c.subscribeTaskCh:
				if subscribeTask.subscribe && !c.subscribed {
					for err = consumer.SubscribeTopics(c.topics, c.rebalanceCb); err != nil; err = consumer.SubscribeTopics(c.topics, c.rebalanceCb) {
						c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
					l1:
						for {
							select {
							case <-time.After(time.Second):
								break l1
							case <-ctx.Done():
								if err = consumer.Close(); err != nil {
									c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
								}
								close(subscribeTask.done)
								return
							}
						}
					}
				} else if !subscribeTask.subscribe && c.subscribed {
					for err = consumer.Unsubscribe(); err != nil; err = consumer.Unsubscribe() {
						c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
					l2:
						for {
							select {
							case <-time.After(time.Second):
								break l2
							case <-ctx.Done():
								if err = consumer.Close(); err != nil {
									c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
								}
								close(subscribeTask.done)
								return
							}
						}
					}
				}
				c.subscribed = subscribeTask.subscribe
				needSubscribing = false
				close(subscribeTask.done)
			default:
				evt := consumer.Poll(100)

				if evt == nil {
					continue
				}

				if stats, ok := evt.(*kafka.Stats); ok {
					if err = handleStatsEvt(stats); err != nil {
						c.events <- kafka.NewError(kafka.ErrApplication, fmt.Sprintf("can't parse statistics: %v", err), false)
					}
				}
				c.events <- evt

				if errEvt, ok := evt.(kafka.Error); ok && errEvt.IsFatal() {
					break loop
				}
			}
		}
	}
}

func (c *Consumer) Subscribe() chan struct{} {
	t := subscribeTask{
		subscribe: true,
		done:      make(chan struct{}),
	}
	c.subscribeTaskCh <- t
	return t.done
}

func (c *Consumer) Unsubscribe() chan struct{} {
	t := subscribeTask{
		subscribe: false,
		done:      make(chan struct{}),
	}
	c.subscribeTaskCh <- t
	return t.done
}

func (c *Consumer) Pause(pause bool, tp kafka.TopicPartition) {
	c.pauseTaskCh <- pauseTask{
		pause: pause,
		tp:    tp,
	}
}

func (c *Consumer) Events() <-chan kafka.Event {
	return c.events
}

type stats struct {
	HandleInstanceName       string                 `json:"name"`
	ClientId                 string                 `json:"client_id"`
	ProducerMessageQueueSize float64                `json:"msg_cnt"`
	BrokersStats             map[string]brokerStats `json:"brokers"`
	CgrpStats                *cgrpStats             `json:"cgrp"`
}

type cgrpStats struct {
	StateAge       float64 `json:"stateage"`
	RebalanceAge   float64 `json:"rebalance_age"`
	RebalanceCount float64 `json:"rebalance_cnt"`
	AssignmentSize float64 `json:"assignment_size"`
}

type brokerStats struct {
	Name                  string      `json:"name"`
	OutBuffQueueSize      float64     `json:"outbuff_cnt"`
	OutMessageQueueSize   float64     `json:"outbuf_msg_cnt"`
	ReqInFlightCount      float64     `json:"waitresp_cnt"`
	MessageInfFlightCount float64     `json:"waitresp_msg_cnt"`
	TxErrorCount          float64     `json:"txerrs"`
	TxRetryCount          float64     `json:"txretries"`
	ReqTimeoutCount       float64     `json:"req_timeouts"`
	RxErrorCount          float64     `json:"rxerrs"`
	RxCorrIdErrorCount    float64     `json:"rxcorriderrs"`
	InternalLatency       windowStats `json:"int_latency"`
	OutBuffLatency        windowStats `json:"outbuf_latency"`
	RoundTripTime         windowStats `json:"rtt"`
}

type windowStats struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
}

func handleStatsEvt(statsEvt *kafka.Stats) error {
	stats := stats{}
	err := json.Unmarshal([]byte(statsEvt.String()), &stats)

	if err != nil {
		return err
	}

	commonLabelValues := []string{stats.HandleInstanceName, stats.ClientId}

	producerMessageQueueSizeGauges.WithLabelValues(commonLabelValues...).Set(stats.ProducerMessageQueueSize)

	if cgrpStats := stats.CgrpStats; cgrpStats != nil {
		consumerStateAgeGauges.WithLabelValues(commonLabelValues...).Set(cgrpStats.StateAge)
		consumerRebalanceAgeGauges.WithLabelValues(commonLabelValues...).Set(cgrpStats.RebalanceAge)
		consumerRebalanceCountGauges.WithLabelValues(commonLabelValues...).Set(cgrpStats.RebalanceCount)
		consumerAssignmentSizeGauges.WithLabelValues(commonLabelValues...).Set(cgrpStats.AssignmentSize)
	}

	for _, brokerStats := range stats.BrokersStats {
		brokerLabelValues := append(commonLabelValues, brokerStats.Name)
		brokerOutBuffQueueSizeGauges.WithLabelValues(brokerLabelValues...).Set(brokerStats.OutBuffQueueSize)
		brokerOutMessageQueueSizeGauges.WithLabelValues(brokerLabelValues...).Set(brokerStats.OutMessageQueueSize)
		brokerReqInFlightCountGauges.WithLabelValues(brokerLabelValues...).Set(brokerStats.ReqInFlightCount)
		brokerMessageInFlightCountGauges.WithLabelValues(brokerLabelValues...).Set(brokerStats.MessageInfFlightCount)
		brokerTxErrorCountGauges.WithLabelValues(brokerLabelValues...).Set(brokerStats.TxErrorCount)
		brokerTxRetryCountGauges.WithLabelValues(brokerLabelValues...).Set(brokerStats.TxRetryCount)
		brokerReqTimeoutCountGauges.WithLabelValues(brokerLabelValues...).Set(brokerStats.ReqTimeoutCount)
		brokerRxErrorCountGauges.WithLabelValues(brokerLabelValues...).Set(brokerStats.RxErrorCount)
		brokerRxCorrIdErrorCountGauges.WithLabelValues(brokerLabelValues...).Set(brokerStats.RxCorrIdErrorCount)
		brokerIntLatencyGauges.WithLabelValues(append(brokerLabelValues, "0.5")...).Set(brokerStats.InternalLatency.P50)
		brokerIntLatencyGauges.WithLabelValues(append(brokerLabelValues, "0.9")...).Set(brokerStats.InternalLatency.P90)
		brokerIntLatencyGauges.WithLabelValues(append(brokerLabelValues, "0.99")...).Set(brokerStats.InternalLatency.P99)
		brokerOutBuffLatencyGauges.WithLabelValues(append(brokerLabelValues, "0.5")...).Set(brokerStats.OutBuffLatency.P50)
		brokerOutBuffLatencyGauges.WithLabelValues(append(brokerLabelValues, "0.9")...).Set(brokerStats.OutBuffLatency.P90)
		brokerOutBuffLatencyGauges.WithLabelValues(append(brokerLabelValues, "0.99")...).Set(brokerStats.OutBuffLatency.P99)
		brokerRttGauges.WithLabelValues(append(brokerLabelValues, "0.5")...).Set(brokerStats.RoundTripTime.P50)
		brokerRttGauges.WithLabelValues(append(brokerLabelValues, "0.9")...).Set(brokerStats.RoundTripTime.P90)
		brokerRttGauges.WithLabelValues(append(brokerLabelValues, "0.99")...).Set(brokerStats.RoundTripTime.P99)
	}

	return nil
}

var producerMessageQueueSizeGauges *prometheus.GaugeVec
var brokerOutBuffQueueSizeGauges *prometheus.GaugeVec
var brokerOutMessageQueueSizeGauges *prometheus.GaugeVec
var brokerReqInFlightCountGauges *prometheus.GaugeVec
var brokerMessageInFlightCountGauges *prometheus.GaugeVec
var brokerTxErrorCountGauges *prometheus.GaugeVec
var brokerTxRetryCountGauges *prometheus.GaugeVec
var brokerReqTimeoutCountGauges *prometheus.GaugeVec
var brokerRxErrorCountGauges *prometheus.GaugeVec
var brokerRxCorrIdErrorCountGauges *prometheus.GaugeVec
var brokerIntLatencyGauges *prometheus.GaugeVec
var brokerOutBuffLatencyGauges *prometheus.GaugeVec
var brokerRttGauges *prometheus.GaugeVec
var consumerStateAgeGauges *prometheus.GaugeVec
var consumerRebalanceAgeGauges *prometheus.GaugeVec
var consumerRebalanceCountGauges *prometheus.GaugeVec
var consumerAssignmentSizeGauges *prometheus.GaugeVec

func init() {
	producerMessageQueueSizeGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "producer",
			Name:      "message_queue_size",
			Help:      "Current number of messages in producer queues",
		}, []string{"handle_instance_name", "client_id"})

	brokerOutBuffQueueSizeGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "outbuff_queue_size",
			Help:      "Number of requests awaiting transmission to broker",
		}, []string{"handle_instance_name", "client_id", "broker_name"})

	brokerOutMessageQueueSizeGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "out_message_queue_size",
			Help:      "Number of messages awaiting transmission to broker",
		}, []string{"handle_instance_name", "client_id", "broker_name"})

	brokerReqInFlightCountGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "req_in_flight_count",
			Help:      "Number of requests in-flight to broker awaiting response",
		}, []string{"handle_instance_name", "client_id", "broker_name"})

	brokerMessageInFlightCountGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "message_in_flight_count",
			Help:      "Number of messages in-flight to broker awaiting response",
		}, []string{"handle_instance_name", "client_id", "broker_name"})

	brokerTxErrorCountGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "tx_error_count",
			Help:      "Total number of transmission errors",
		}, []string{"handle_instance_name", "client_id", "broker_name"})

	brokerTxRetryCountGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "tx_retry_count",
			Help:      "Total number of request retries",
		}, []string{"handle_instance_name", "client_id", "broker_name"})

	brokerReqTimeoutCountGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "req_timeout_count",
			Help:      "Total number of requests timed out",
		}, []string{"handle_instance_name", "client_id", "broker_name"})

	brokerRxErrorCountGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "rx_error_count",
			Help:      "Total number of receive errors",
		}, []string{"handle_instance_name", "client_id", "broker_name"})

	brokerRxCorrIdErrorCountGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "rx_corr_id_error_count",
			Help:      "Total number of unmatched correlation ids in response (typically for timed out requests)",
		}, []string{"handle_instance_name", "client_id", "broker_name"})

	brokerIntLatencyGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "internal_latency",
			Help:      "Internal producer queue latency in microseconds",
		}, []string{"handle_instance_name", "client_id", "broker_name", "quantile"})

	brokerOutBuffLatencyGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "outbuff_latency",
			Help: "Internal request queue latency in microseconds. " +
				"This is the time between a request is enqueued on the transmit (outbuf) queue and the time the request is written to the TCP socket. " +
				"Additional buffering and latency may be incurred by the TCP stack and network.",
		}, []string{"handle_instance_name", "client_id", "broker_name", "quantile"})

	brokerRttGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "broker",
			Name:      "round_trip_time",
			Help:      "Broker latency / round-trip time in microseconds.",
		}, []string{"handle_instance_name", "client_id", "broker_name", "quantile"})

	consumerStateAgeGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "consumer",
			Name:      "state_age",
			Help:      "Time elapsed since last state change (milliseconds)",
		}, []string{"handle_instance_name", "client_id"})

	consumerRebalanceAgeGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "consumer",
			Name:      "rebalance_age",
			Help:      "Time elapsed since last rebalance (assign or revoke) (milliseconds)",
		}, []string{"handle_instance_name", "client_id"})

	consumerRebalanceCountGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "consumer",
			Name:      "rebalance_count",
			Help:      "Total number of rebalances (assign or revoke)",
		}, []string{"handle_instance_name", "client_id"})

	consumerAssignmentSizeGauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "kafka",
			Subsystem: "consumer",
			Name:      "assignment_size",
			Help:      "Current assignment's partition count",
		}, []string{"handle_instance_name", "client_id"})

	prometheus.MustRegister(producerMessageQueueSizeGauges, brokerOutBuffQueueSizeGauges, brokerOutMessageQueueSizeGauges,
		brokerReqInFlightCountGauges, brokerMessageInFlightCountGauges, brokerTxErrorCountGauges, brokerTxRetryCountGauges,
		brokerReqTimeoutCountGauges, brokerRxErrorCountGauges, brokerRxCorrIdErrorCountGauges, brokerIntLatencyGauges,
		brokerOutBuffLatencyGauges, brokerRttGauges, consumerStateAgeGauges, consumerRebalanceAgeGauges,
		consumerRebalanceCountGauges, consumerAssignmentSizeGauges)
}
