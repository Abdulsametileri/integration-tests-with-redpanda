package kafka

import segmentio "github.com/segmentio/kafka-go"

type Message struct {
	Key   []byte
	Value []byte
	Topic string
}

func (m *Message) To() segmentio.Message {
	return segmentio.Message{
		Key:   m.Key,
		Value: m.Value,
		Topic: m.Topic,
	}
}

func From(sm segmentio.Message) Message {
	return Message{
		Key:   sm.Key,
		Value: sm.Value,
		Topic: sm.Topic,
	}
}
