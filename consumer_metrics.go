package pubsub

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/mo"
)

func NewConsumerOptionsMetricsThresholdWarning(v float64) consumerOptionsMetricsThreshold {
	return consumerOptionsMetricsThreshold{
		warning: mo.Some(mo.Left[float64, func() float64](v)),
	}
}

func NewConsumerOptionsMetricsThresholdError(v float64) consumerOptionsMetricsThreshold {
	return consumerOptionsMetricsThreshold{
		eRror: mo.Some(mo.Left[float64, func() float64](v)),
	}
}

func NewConsumerOptionsMetricsThresholdWarningAndError(v1 float64, v2 float64) consumerOptionsMetricsThreshold {
	return consumerOptionsMetricsThreshold{
		warning: mo.Some(mo.Left[float64, func() float64](v1)),
		eRror:   mo.Some(mo.Left[float64, func() float64](v2)),
	}
}

func NewConsumerOptionsMetricsThresholdWarningFunc(f func() float64) consumerOptionsMetricsThreshold {
	return consumerOptionsMetricsThreshold{
		warning: mo.Some(mo.Right[float64, func() float64](f)),
	}
}

func NewConsumerOptionsMetricsThresholdErrorFunc(f func() float64) consumerOptionsMetricsThreshold {
	return consumerOptionsMetricsThreshold{
		eRror: mo.Some(mo.Right[float64, func() float64](f)),
	}
}

func NewConsumerOptionsMetricsThresholdWarningFuncAndErrorFunc(f1 func() float64, f2 func() float64) consumerOptionsMetricsThreshold {
	return consumerOptionsMetricsThreshold{
		warning: mo.Some(mo.Right[float64, func() float64](f1)),
		eRror:   mo.Some(mo.Right[float64, func() float64](f2)),
	}
}

type consumerOptionsMetricsThreshold struct {
	warning mo.Option[mo.Either[float64, func() float64]]
	eRror   mo.Option[mo.Either[float64, func() float64]]
}

func (opt consumerOptionsMetricsThreshold) metrics(name string, description string, consumerName string) []*metric {
	metrics := []*metric{}

	if w, ok := opt.warning.Get(); ok {
		metrics = append(metrics, newMetric(
			name+"_warning",
			description,
			consumerName,
			w,
		))
	}

	if e, ok := opt.eRror.Get(); ok {
		metrics = append(metrics, newMetric(
			name+"_error",
			description,
			consumerName,
			e,
		))
	}

	return metrics
}

type ConsumerOptionsMetrics struct {
	QueueMessageBytesThreshold consumerOptionsMetricsThreshold
	QueueMessagesThreshold     consumerOptionsMetricsThreshold

	DeadLetterQueueMessageBytesThreshold consumerOptionsMetricsThreshold
	DeadLetterQueueMessagesThreshold     consumerOptionsMetricsThreshold
	DeadLetterQueueMessageRateThreshold  consumerOptionsMetricsThreshold

	RetryQueueMessageBytesThreshold consumerOptionsMetricsThreshold
	RetryQueueMessagesThreshold     consumerOptionsMetricsThreshold
	RetryQueueMessageRateThreshold  consumerOptionsMetricsThreshold
}

func (opt ConsumerOptionsMetrics) metrics(consumerName string) []*metric {
	metrics := []*metric{}

	metrics = append(metrics, opt.QueueMessageBytesThreshold.metrics("queue_message_bytes_threshold", "", consumerName)...)
	metrics = append(metrics, opt.QueueMessagesThreshold.metrics("queue_messages_threshold", "", consumerName)...)

	metrics = append(metrics, opt.DeadLetterQueueMessageBytesThreshold.metrics("deadletter_queue_message_bytes_threshold", "", consumerName)...)
	metrics = append(metrics, opt.DeadLetterQueueMessagesThreshold.metrics("deadletter_queue_messages_threshold", "", consumerName)...)
	metrics = append(metrics, opt.DeadLetterQueueMessageRateThreshold.metrics("deadletter_queue_messages_rate_threshold", "", consumerName)...)

	metrics = append(metrics, opt.RetryQueueMessageBytesThreshold.metrics("retry_queue_message_bytes_threshold", "", consumerName)...)
	metrics = append(metrics, opt.RetryQueueMessagesThreshold.metrics("retry_queue_messages_threshold", "", consumerName)...)
	metrics = append(metrics, opt.RetryQueueMessageRateThreshold.metrics("retry_queue_messages_rate_threshold", "", consumerName)...)

	return metrics
}

func newMetric(name string, description string, consumer string, value mo.Either[float64, func() float64]) *metric {
	return &metric{
		value:    value,
		consumer: consumer,
		desc: prometheus.NewDesc(
			name,
			description,
			[]string{"consumer"},
			nil,
		),
	}
}

type metric struct {
	value    mo.Either[float64, func() float64]
	consumer string
	desc     *prometheus.Desc
}

func (m *metric) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.desc
}

func (m *metric) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		m.desc,
		prometheus.GaugeValue,
		eitherToValue(m.value),
		m.consumer,
	)
}

func eitherToValue(o mo.Either[float64, func() float64]) float64 {
	if v, ok := o.Left(); ok {
		return v
	}

	return o.MustRight()()
}
