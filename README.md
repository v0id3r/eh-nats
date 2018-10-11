# eh-nats

Eventbus with NATS/NATS Streaming backend for https://github.com/looplab/eventhorizon

# Example init
```
nc, err := nats.Connect("nats://localhost:4222")
conn, err := stan.Connect("test-cluster", "client", stan.NatsConn(nc))
eventBus, err := nats.NewEventBus(conn, "bus")
```
