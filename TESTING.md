
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
