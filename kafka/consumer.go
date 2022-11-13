package kafka

import (
	"context"
	segmentio "github.com/segmentio/kafka-go"
)

type Consumer struct {
	r *segmentio.Reader
}

func NewConsumer(cfg *Config) *Consumer {
	return &Consumer{r: segmentio.NewReader(segmentio.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     cfg.Consumer.GroupID,
		GroupTopics: []string{cfg.Consumer.Topic},
	})}
}

func (k *Consumer) Consume(ctx context.Context) (Message, error) {
	message, err := k.r.ReadMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	return From(message), nil
}
