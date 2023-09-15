package pubsub

import "github.com/samber/mo"

prévoir un % nack/total
func NewconsumerMetricsThresholdWarning(v int) consumerMetricsThreshold {
	return consumerMetricsThreshold{
		Warning: mo.Some[int](v),
	}
}

func NewconsumerMetricsThresholdError(v int) consumerMetricsThreshold {
	return consumerMetricsThreshold{
		Error: mo.Some[int](v),
	}
}

func NewconsumerMetricsThresholdWarningAndError(v1 int, v2 int) consumerMetricsThreshold {
	return consumerMetricsThreshold{
		Warning: mo.Some[int](v1),
		Error:   mo.Some[int](v2),
	}
}

func NewconsumerMetricsThresholdWarningFunc(f func() int) consumerMetricsThreshold {
	return consumerMetricsThreshold{
		WarningFunc: mo.Some[func() int](f),
	}
}

func NewconsumerMetricsThresholdErrorFunc(f func() int) consumerMetricsThreshold {
	return consumerMetricsThreshold{
		ErrorFunc: mo.Some[func() int](f),
	}
}

func NewconsumerMetricsThresholdWarningFuncAndErrorFunc(f1 func() int, f2 func() int) consumerMetricsThreshold {
	return consumerMetricsThreshold{
		WarningFunc: mo.Some[func() int](f1),
		ErrorFunc:   mo.Some[func() int](f2),
	}
}

type consumerMetricsThreshold struct {
	Warning     mo.Option[int]
	WarningFunc mo.Option[func() int]

	Error     mo.Option[int]
	ErrorFunc mo.Option[func() int]
}

type ConsumerMetrics struct {
	QueueMessagesThreshold               consumerMetricsThreshold
	QueueMessageBytesThreshold           consumerMetricsThreshold
	DeadLetterQueueMessagesThreshold     consumerMetricsThreshold
	DeadLetterQueueMessageBytesThreshold consumerMetricsThreshold
	RetryQueueMessagesThreshold          consumerMetricsThreshold
	RetryQueueMessageBytesThreshold      consumerMetricsThreshold
}
