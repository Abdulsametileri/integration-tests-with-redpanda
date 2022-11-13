package kafka

type Config struct {
	Brokers  []string
	Consumer ConsumerConfig
}

type ConsumerConfig struct {
	GroupID string
	Topic   string
}
