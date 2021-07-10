package pubsub

import "log"

var logger = log.Default()

func SetLogger(l *log.Logger) {
	logger = l
}
