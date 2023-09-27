
# Some manual tests

## Handle rabbitmq restart

- start consumers
- start producers
- stop rabbitmq properly
- start rabbitmq

## Handle short network failure

- start consumers
- start producers
- kill rabbitmq
- start rabbitmq

## Handle network failure on app start

- stop rabbitmq
- start consumers
- start producers
- start rabbitmq

## Handle message timeout

- insert a timeout in a message

## Handle consumer stop

- create a consumer catching ctrl-c event
- run producer
- run a slow consumer (with buffered messages)
- ctrl-c

## Handle exchange/queue removal

- run producer
- run consumer
- remove some bindings
- remove queue
- remove exchange
