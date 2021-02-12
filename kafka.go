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

	ready := make(chan struct{})
	var whyNotReady error
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	var producer *kafka.Producer

	producer, whyNotReady = kafka.NewProducer(p.config)

	if whyNotReady == nil {
		close(ready)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for ; whyNotReady != nil; {
			mu.Lock()
			producer, whyNotReady = kafka.NewProducer(p.config)
			mu.Unlock()
			if ctx.Err() != nil {
				return
			}
			time.Sleep(time.Second)
		}

		select {
		case <-ready:
		default:
			close(ready)
		}

		for evt := range producer.Events() {
			if stats, ok := evt.(*kafka.Stats); ok {
				if err := handleStatsEvt(stats); err != nil {
					p.events <- kafka.NewError(kafka.ErrApplication, fmt.Sprintf("can't parse statistics: %v", err), false)
				}
			}
			p.events <- evt
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			close(p.input)
		}
	}()

	for msg := range p.input {
		select {
		case <-ready:
			producer.ProduceChannel() <- msg
		default:
			mu.Lock()
			msg.TopicPartition.Error = whyNotReady
			mu.Unlock()
			p.events <- msg
		}
	}

	producer.Flush(1000)
	producer.Close()

	wg.Wait()
}

func (p *Producer) Input() chan<- *kafka.Message {
	return p.input
}

func (p *Producer) Events() <-chan kafka.Event {
	return p.events
}

type Consumer struct {
	messages chan *kafka.Message
	events   chan kafka.Event
	config   *kafka.ConfigMap
	topics   []string
}

func NewConsumer(config *kafka.ConfigMap, topics []string) *Consumer {
	return &Consumer{
		messages: make(chan *kafka.Message, chanBuffSize),
		events:   make(chan kafka.Event, chanBuffSize),
		config:   config,
		topics:   topics,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	defer close(c.events)
	defer close(c.messages)

	var consumer *kafka.Consumer
	var err error
	for consumer, err = kafka.NewConsumer(c.config); err != nil; consumer, err = kafka.NewConsumer(c.config) {
		c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}

	for err = consumer.SubscribeTopics(c.topics, nil); err != nil; err = consumer.SubscribeTopics(c.topics, nil) {
		c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			if err = consumer.Close(); err != nil {
				c.events <- kafka.NewError(kafka.ErrApplication, err.Error(), false)
			}
			return
		default:
			evt := consumer.Poll(100)

			if evt == nil {
				continue
			}

			switch e := evt.(type) {
			case *kafka.Message:
				c.messages <- e
			default:
				if stats, ok := evt.(*kafka.Stats); ok {
					if err = handleStatsEvt(stats); err != nil {
						c.events <- kafka.NewError(kafka.ErrApplication, fmt.Sprintf("can't parse statistics: %v", err), false)
					}
				}
				c.events <- evt
			}
		}
	}
}

func (c *Consumer) Messages() <-chan *kafka.Message {
	return c.messages
}

func (c *Consumer) Events() <-chan kafka.Event {
	return c.events
}

type stats struct {
	HandleInstanceName       string  `json:"name"`
	ClientId                 string  `json:"client_id"`
	ProducerMessageQueueSize float64 `json:"msg_cnt"`
	Brokers                  struct {
		Stats []brokerStats
	} `json:"brokers"`
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

	producerMessageQueueSizeGauges.WithLabelValues(stats.HandleInstanceName, stats.ClientId).Set(stats.ProducerMessageQueueSize)

	for _, brokerStats := range stats.Brokers.Stats {
		brokerLabelValues := []string{stats.HandleInstanceName, stats.ClientId, brokerStats.Name}
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

	prometheus.MustRegister(producerMessageQueueSizeGauges, brokerOutBuffQueueSizeGauges, brokerOutMessageQueueSizeGauges,
		brokerReqInFlightCountGauges, brokerMessageInFlightCountGauges, brokerTxErrorCountGauges, brokerTxRetryCountGauges,
		brokerReqTimeoutCountGauges, brokerRxErrorCountGauges, brokerRxCorrIdErrorCountGauges, brokerIntLatencyGauges,
		brokerOutBuffLatencyGauges, brokerRttGauges)
}
