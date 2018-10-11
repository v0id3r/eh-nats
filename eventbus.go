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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/globalsign/mgo/bson"

	eh "github.com/looplab/eventhorizon"
	"github.com/nats-io/go-nats-streaming"
)

// ErrCouldNotMarshalEvent is when an event could not be marshaled into BSON.
func ErrCouldNotMarshalEvent(err error) error {
	return fmt.Errorf("could not marshal event: %v", err.Error())
}

// ErrCouldNotUnmarshalEvent is when an event could not be unmarshaled into BSON.
func ErrCouldNotUnmarshalEvent(err error) error {
	return fmt.Errorf("could not unmarshal event: %v", err.Error())
}

// ErrCouldNotPublishEvent is when kafka-client cannot send event to kafka
func ErrCouldNotPublishEvent(err error) error {
	return fmt.Errorf("could not unmarshal event: %v", err.Error())
}

// Error is an async error containing the error and the event.
type Error struct {
	Err   error
	Ctx   context.Context
	Event eh.Event
}

// Error implements the Error method of the error interface.
func (e Error) Error() string {
	return fmt.Sprintf("%s: (%s)", e.Err, e.Event.String())
}

type EventBus struct {
	conn          stan.Conn
	subjectPrefix string
	registered    map[eh.EventHandlerType]struct{}
	registeredMu  sync.RWMutex
	errCh         chan Error
}

func NewEventBus(nc stan.Conn, subjectPrefix string) (*EventBus, error) {
	return &EventBus{
		conn:          nc,
		subjectPrefix: subjectPrefix,
		registered:    map[eh.EventHandlerType]struct{}{},
		errCh:         make(chan Error, 100),
	}, nil
}

type Event struct {
	EventType     eh.EventType
	Timestamp     time.Time
	AggregateType eh.AggregateType
	AggregateID   eh.UUID
	Version       int
	Context       map[string]interface{}
	RawData       *bson.Raw `bson:",omitempty"`
}

func (b *EventBus) PublishEvent(ctx context.Context, event eh.Event) error {
	wireEvent := &Event{
		EventType:     event.EventType(),
		AggregateType: event.AggregateType(),
		AggregateID:   event.AggregateID(),
		Version:       event.Version(),
		Timestamp:     event.Timestamp(),
		Context:       eh.MarshalContext(ctx),
	}

	if event.Data() != nil {
		bytesData, err := bson.Marshal(event.Data())
		if err != nil {
			return ErrCouldNotMarshalEvent(err)
		}
		wireEvent.RawData = &bson.Raw{Kind: bson.ElementDocument, Data: bytesData}
	}

	marshaledEvent, err := bson.Marshal(wireEvent)
	if err != nil {
		return ErrCouldNotMarshalEvent(err)
	}

	t := b.subjectPrefix
	err = b.conn.Publish(t, marshaledEvent)

	if err != nil {
		return ErrCouldNotPublishEvent(err)
	}

	return nil
}

// AddHandler implements the AddHandler method of the eventhorizon.EventBus interface.
func (b *EventBus) AddHandler(m eh.EventMatcher, h eh.EventHandler) {
	_, err := b.subscription(m, h, false)
	if err != nil {
		b.errCh <- Error{Err: err}
	}
}

// AddObserver implements the AddObserver method of the eventhorizon.EventBus interface.
func (b *EventBus) AddObserver(m eh.EventMatcher, h eh.EventHandler) {
	_, err := b.subscription(m, h, true)
	if err != nil {
		b.errCh <- Error{Err: err}
	}
}

// Errors returns an error channel where async handling errors are sent.
func (b *EventBus) Errors() <-chan Error {
	return b.errCh
}

func (b *EventBus) newMessageHandler(matcher eh.EventMatcher, handler eh.EventHandler) stan.MsgHandler {
	return func(m *stan.Msg) {
		var e Event
		if err := bson.Unmarshal(m.Data, &e); err != nil {
			select {
			case b.errCh <- Error{Err: ErrCouldNotUnmarshalEvent(err)}:
			default:
			}
			return
		}

		var data eh.EventData
		if e.RawData != nil {
			var err error
			if data, err = eh.CreateEventData(e.EventType); err != nil {
				select {
				case b.errCh <- Error{Err: ErrCouldNotUnmarshalEvent(err)}:
				default:
				}
				return
			}
			if err := e.RawData.Unmarshal(data); err != nil {
				select {
				case b.errCh <- Error{Err: ErrCouldNotUnmarshalEvent(err)}:
				default:
				}
				return
			}
		}

		ehEvent := eh.NewEventForAggregate(e.EventType, data, e.Timestamp, e.AggregateType, e.AggregateID, e.Version)

		if !matcher(ehEvent) {
			m.Ack()
			return
		}

		ctx := eh.UnmarshalContext(e.Context)
		// Notify all observers about the event.
		if err := handler.HandleEvent(ctx, ehEvent); err != nil {
			select {
			case b.errCh <- Error{Err: fmt.Errorf("could not handle event (%s): %s", handler.HandlerType(), err.Error()), Ctx: ctx, Event: ehEvent}:
			default:
			}
			return
		}
		m.Ack()
		return
	}
}

// Checks the matcher and handler and gets the event subscription.
func (b *EventBus) subscription(m eh.EventMatcher, h eh.EventHandler, observer bool) (stan.Subscription, error) {
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()

	if m == nil {
		panic("matcher can't be nil")
	}
	if h == nil {
		panic("handler can't be nil")
	}
	if _, ok := b.registered[h.HandlerType()]; ok {
		panic(fmt.Sprintf("multiple registrations for %s", h.HandlerType()))
	}
	b.registered[h.HandlerType()] = struct{}{}

	handlerType := string(h.HandlerType())

	handler := b.newMessageHandler(m, h)
	subject := b.subjectPrefix

	var sub stan.Subscription
	var err error
	if observer {
		sub, err = b.conn.Subscribe(subject, handler, stan.SetManualAckMode())
	} else {
		sub, err = b.conn.QueueSubscribe(subject, handlerType, handler, stan.SetManualAckMode())
	}

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to create subscription fo %v: %v", h.HandlerType(), err))
	}

	return sub, nil
}
