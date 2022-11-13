package kafka

import (
	"context"
	segmentio "github.com/segmentio/kafka-go"
)

type Producer struct {
	w *segmentio.Writer
}

func NewProducer(cfg *Config) *Producer {
	return &Producer{
		w: &segmentio.Writer{
			Addr:                   segmentio.TCP(cfg.Brokers...),
			AllowAutoTopicCreation: true,
		},
	}
}

func (k *Producer) Produce(ctx context.Context, message Message) error {
	return k.w.WriteMessages(ctx, message.To())
}
