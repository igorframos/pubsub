package pubsub

import (
	"fmt"
	"strings"
	"sync"
)

type Controller struct {
	// BufferSize is the size of the buffer for each channel. This represents the maximum number of unprocessed messages
	// before the subscription will no longer receive new publications.
	BufferSize int

	// subs stores all the subscriptions for each topic. It is used to publish to all topics.
	subs map[string][]*subscription

	// subByID allows us to close the channel when someone is trying to get rid of a subscription.
	subByID map[uint64]*subscription

	// nextID will allow us to give a unique ID to each subscription so we can find it in all topics.
	nextID uint64

	// mutex is used for mutual exclusion when altering subs.
	mutex sync.RWMutex
}

func New(bufferSize int) *Controller {
	return &Controller{
		BufferSize: bufferSize,
		subs:       make(map[string][]*subscription),
		subByID:    make(map[uint64]*subscription),
	}
}

type Subscription interface {
	ID() uint64
	Topics() []string
	C() <-chan interface{}
}

func (c *Controller) Publish(topic string, data interface{}) error {
	var errors []string
	for _, s := range c.subs[topic] {
		if err := nonBlockingSend(s.c, data); err != nil {
			errors = append(errors, fmt.Sprintf("Subscription %d: %v", s.ID(), err))
		}
	}

	if len(errors) == 0 {
		return nil
	}

	if len(errors) == 1 {
		return fmt.Errorf("1 error when publishing to topic %s: %v", topic, errors[0])
	}

	return fmt.Errorf("%d errors publishing to topic %q:\n   %v", len(errors), topic, strings.Join(errors, "\n   "))
}

func (c *Controller) Subscribe(topics []string) Subscription {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.nextID++
	s := &subscription{
		id:     c.nextID,
		topics: topics,
		c:      make(chan interface{}, c.BufferSize),
	}

	c.subByID[s.ID()] = s
	for _, topic := range topics {
		c.subs[topic] = append(c.subs[topic], s)
	}

	return s
}

func (c *Controller) Unsubscribe(s Subscription) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, t := range s.Topics() {
		c.subs[t] = purge(c.subs[t], s)
		if len(c.subs[t]) == 0 {
			delete(c.subs, t)
		}
	}

	close(c.subByID[s.ID()].c)
	delete(c.subByID, s.ID())
}

// subscription implements Subscription. This ensures the struct is immutable and that only this package is able to send
// in the channel held by the struct. Anyone outside this package will only see a Subscription, which returns a
// receive-only channel.
type subscription struct {
	id     uint64
	topics []string
	c      chan interface{}
}

// ID is a getter for the id property.
func (s *subscription) ID() uint64 {
	return s.id
}

// Topics is a getter for the topics property.
func (s *subscription) Topics() []string {
	return s.topics
}

// C is a getter for the c property.
func (s *subscription) C() <-chan interface{} {
	return s.c
}

func nonBlockingSend(c chan<- interface{}, data interface{}) error {
	select {
	case c <- data:
		return nil
	default:
		return fmt.Errorf("Channel was not ready")
	}
}

func purge(subs []*subscription, s Subscription) []*subscription {
	for i, sub := range subs {
		if sub.ID() == s.ID() {
			return append(subs[:i], subs[i+1:]...)
		}
	}

	return subs
}
