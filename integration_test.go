package integration_tests_with_redpanda

import (
	"context"
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
	CleanUp()
	GetBrokerAddresses() []string
}

type IntegrationTestSuite struct {
	suite.Suite
	lib IntegrationLibraryStrategy
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	s := new(IntegrationTestSuite)
	s.lib = &DockerTestStrategy{}

	suite.Run(t, s)
}

func (s *IntegrationTestSuite) SetupSuite() {
	if err := s.lib.RunContainer(); err != nil {
		log.Fatalln(err)
	}
}

func (s *IntegrationTestSuite) TearDownSuite() {
	s.lib.CleanUp()
}

func (s *IntegrationTestSuite) Test_Should_Consume_Successfully() {
	// Given
	cfg := &kafka.Config{
		Brokers: s.lib.GetBrokerAddresses(),
		Consumer: kafka.ConsumerConfig{
			GroupID: "consumer-group-1",
			Topic:   "test-consume",
		},
	}
	producer := kafka.NewProducer(cfg)
	consumer := kafka.NewConsumer(cfg)

	expectedMessage := kafka.Message{Key: nil, Value: []byte(`{ "say": "hello" }`), Topic: cfg.Consumer.Topic}
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

func (s *IntegrationTestSuite) Test_Should_Produce_Successfully() {
	// Given
	cfg := &kafka.Config{
		Brokers: s.lib.GetBrokerAddresses(),
		Consumer: kafka.ConsumerConfig{
			GroupID: "consumer-group-2",
			Topic:   "test-produce",
		},
	}
	producer := kafka.NewProducer(cfg)
	expectedMessage := kafka.Message{Key: nil, Value: []byte(`{ "say": "hello" }`), Topic: cfg.Consumer.Topic}

	// When
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	err := producer.Produce(ctx, expectedMessage)

	// Then
	assert.Nil(s.T(), err)
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
