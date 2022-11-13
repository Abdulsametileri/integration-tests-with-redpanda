package integration_tests_with_redpanda

import (
	"context"
	"fmt"
	"github.com/Abdulsametleri/integration-tests-with-redpanda/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"log"
	"net"
	"testing"
	"time"
)

type IntegrationLibraryStrategy interface {
	RunContainer() error
	Cleanup()
	GetHostPort() int
}

type IntegrationTestSuite struct {
	suite.Suite
	lib IntegrationLibraryStrategy
	cfg *kafka.Config
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	s := new(IntegrationTestSuite)
	s.lib = &TestContainerStrategy{}

	suite.Run(t, s)
}

func (s *IntegrationTestSuite) SetupSuite() {
	if err := s.lib.RunContainer(); err != nil {
		log.Fatalln(err)
	}

	s.cfg = &kafka.Config{
		Brokers: []string{fmt.Sprintf("localhost:%d", s.lib.GetHostPort())},
		Consumer: kafka.ConsumerConfig{
			GroupID: "example-group",
			Topic:   "example-topic",
		},
	}
}

func (s *IntegrationTestSuite) TearDownSuite() {
	s.lib.Cleanup()
}

func (s *IntegrationTestSuite) Test_Should_Consume_Successfully() {
	// Given
	producer := kafka.NewProducer(s.cfg)
	consumer := kafka.NewConsumer(s.cfg)

	expectedMessage := kafka.Message{Key: nil, Value: []byte(`{ "say": "hello" }`), Topic: s.cfg.Consumer.Topic}
	err := producer.Produce(context.Background(), expectedMessage)
	if err != nil {
		s.T().Fatalf("could not produce example message %s", err)
	}

	// When
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	actualMessage, err := consumer.Consume(ctx)

	// Then
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), expectedMessage, actualMessage)
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
