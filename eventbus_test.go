// Copyright 2018 - Alexey Karnov (void.alexey@gmail.com)

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package nats

import (
	"os"
	"testing"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/eventbus"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats"
)

func TestEventBus(t *testing.T) {
	// Connect to localhost if not running inside docker
	natsUrl := os.Getenv("NATS_ADDR")
	clusterId := os.Getenv("STAN_CLUSTER_ID")
	clientId := os.Getenv("STAN_CLIENT_ID")

	if natsUrl == "" {
		natsUrl = "nats://localhost:4222"
	}

	if clusterId == "" {
		clusterId = "test-cluster"
	}

	if clientId == "" {
		clientId = eh.NewUUID().String()
	}

	topic := eh.NewUUID().String()

	nc, err := nats.Connect(natsUrl,
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			t.Error(err)
		}),
	)
	if err != nil {
		t.Error(err)
	}

	c, err := stan.Connect(clusterId, clientId, stan.NatsConn(nc))
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	bus1, err := NewEventBus(c, topic)
	if err != nil {
		t.Error(err)
	}

	bus2, err := NewEventBus(c, topic)
	if err != nil {
		t.Error(err)
	}

	eventbus.AcceptanceTest(t, bus1, bus2)

}
