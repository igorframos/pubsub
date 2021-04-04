package pubsub

import (
	"fmt"
	"testing"
)

const (
	topic1 = "topic1"
	topic2 = "topic2"
	topic3 = "topic3"

	data1 = "test data 1"
	data2 = "test data 2"
	data3 = "test data 3"

	bufferSize = 2
)

var (
	pubsub = New(bufferSize)
)

func TestSingleSubscription(t *testing.T) {
	s := pubsub.Subscribe([]string{topic1})

	t.Run("Receives publication in the same topic", func(t *testing.T) {
		pubsub.Publish(topic1, data1)
		if err := receiveString(s, data1); err != nil {
			t.Error(err)
		}
	})

	t.Run("Does not receive publication in a different topic", func(t *testing.T) {
		pubsub.Publish(topic2, data2)
		if err := dontReceive(s); err != nil {
			t.Error(err)
		}
	})

	t.Run("Can be removed", func(t *testing.T) {
		pubsub.Unsubscribe(s)

		pubsub.Publish(topic1, data1)
		if err := dontReceive(s); err != nil {
			t.Error(err)
		}
	})
}

func TestSubscriptionForMultipleTopics(t *testing.T) {
	s := pubsub.Subscribe([]string{topic1, topic2})

	t.Run("Receives publication in both topics", func(t *testing.T) {
		pubsub.Publish(topic1, data1)
		if err := receiveString(s, data1); err != nil {
			t.Error(err)
		}

		pubsub.Publish(topic2, data2)
		if err := receiveString(s, data2); err != nil {
			t.Error(err)
		}
	})

	t.Run("Does not receive publication in a different topic", func(t *testing.T) {
		pubsub.Publish(topic3, data3)
		if err := dontReceive(s); err != nil {
			t.Error(err)
		}
	})

	t.Run("Can be removed", func(t *testing.T) {
		pubsub.Unsubscribe(s)

		pubsub.Publish(topic1, data1)
		if err := dontReceive(s); err != nil {
			t.Error(err)
		}

		pubsub.Publish(topic2, data2)
		if err := dontReceive(s); err != nil {
			t.Error(err)
		}
	})
}

func TestMultipleSubscriptions(t *testing.T) {
	s1 := pubsub.Subscribe([]string{topic1, topic2})
	s2 := pubsub.Subscribe([]string{topic2})

	t.Run("Only subscriptions for a topic receive publications for it", func(t *testing.T) {
		pubsub.Publish(topic1, data1)

		if err := receiveString(s1, data1); err != nil {
			t.Error(err)
		}

		if err := dontReceive(s2); err != nil {
			t.Error(err)
		}
	})

	t.Run("All subscriptions for a topic receive publications for it", func(t *testing.T) {
		pubsub.Publish(topic2, data2)

		if err := receiveString(s1, data2); err != nil {
			t.Error(err)
		}

		if err := receiveString(s2, data2); err != nil {
			t.Error(err)
		}
	})

	t.Run("Removing one subscription does not affect the other", func(t *testing.T) {
		pubsub.Unsubscribe(s2)

		pubsub.Publish(topic1, data1)

		if err := receiveString(s1, data1); err != nil {
			t.Error(err)
		}

		if err := dontReceive(s2); err != nil {
			t.Error(err)
		}

		pubsub.Publish(topic2, data2)

		if err := receiveString(s1, data2); err != nil {
			t.Error(err)
		}

		if err := dontReceive(s2); err != nil {
			t.Error(err)
		}
	})

	t.Run("All subscriptions can be removed", func(t *testing.T) {
		pubsub.Unsubscribe(s1)

		pubsub.Publish(topic2, data2)

		if err := dontReceive(s1); err != nil {
			t.Error(err)
		}

		if err := dontReceive(s2); err != nil {
			t.Error(err)
		}
	})
}

func TestBuffer(t *testing.T) {
	_ = pubsub.Subscribe([]string{topic1, topic2})

	t.Run("We can publish without error until the buffer is full", func(t *testing.T) {
		for i := 0; i < bufferSize; i++ {
			if err := pubsub.Publish(topic1, data1); err != nil {
				t.Error(err)
			}
		}
	})

	t.Run("When the buffer is full, Publish() will report an error", func(t *testing.T) {
		if err := pubsub.Publish(topic1, data1); err == nil {
			t.Error("Expected to get an error from the buffer being full, but publication was accepted")
		}
	})
}

func receiveString(s Subscription, expected string) error {
	select {
	case data, ok := <-s.C():
		if !ok {
			return fmt.Errorf("Expeceted to receive data from subscription %d, but the channel is closed", s.ID())
		}

		str, ok := data.(string)
		if !ok {
			return fmt.Errorf("Received data from subscription %d, but it is not a string", s.ID())
		}

		if str != expected {
			return fmt.Errorf("Incorrect data received by subscription %d. Expected: %q. Got: %q", s.ID(), expected, str)
		}
	default:
		return fmt.Errorf("Timed out waiting for string %q", expected)
	}

	return nil
}

func dontReceive(s Subscription) error {
	select {
	case data, ok := <-s.C():
		if ok {
			return fmt.Errorf("Expected not to receive data, but got %q", data)
		}

		// This is not a receive. It just means the channel has been closed.
		return nil
	default:
		return nil
	}
}
