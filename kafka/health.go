package kafka

import "github.com/segmentio/kafka-go"

func CheckHealth(port string) error {
	conn, err := kafka.Dial("tcp", ":"+port)
	if err != nil {
		return err
	}
	// Read metadata to ensure Kafka is available.
	_, err = conn.Brokers()
	return err
}
