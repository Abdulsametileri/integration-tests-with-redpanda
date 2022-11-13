package kafka

import (
	"fmt"
	"github.com/segmentio/kafka-go"
)

func CheckHealth(port int) error {
	conn, err := kafka.Dial("tcp", ":"+fmt.Sprintf("%d", port))
	if err != nil {
		return err
	}
	// Read metadata to ensure Kafka is available.
	_, err = conn.Brokers()
	return err
}
