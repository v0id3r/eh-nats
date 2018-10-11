# eh-nats

Eventbus with NATS/NATS Streaming backend for https://github.com/looplab/eventhorizon

## Example init
### Connection configuration
```
natsUrl := "nats://localhost:4222"
subject := "ehBus"
clusterId := "test-cluster"
clientId := "client-id"
```
### Create connection
```
nc, err := nats.Connect(natsUrl)
conn, err := stan.Connect(clusterId, clientId, stan.NatsConn(nc))
```
or
```
conn, err := stan.Connect(clusterId, clientId, stan.NatsURL(natsUrl))
```
### Create bus using connection
```
eventBus, err := nats.NewEventBus(conn, subject)
```
