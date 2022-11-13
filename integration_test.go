package integration_tests_with_redpanda_test

import (
	"context"
	"fmt"
	"github.com/Abdulsametleri/integration-tests-with-redpanda/kafka"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

type IntegrationTestSuite struct {
	suite.Suite
	wrapper *DockerTestWrapper
	cfg     *kafka.Config
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	suite.Run(t, new(IntegrationTestSuite))
}

func (s *IntegrationTestSuite) SetupSuite() {
	var err error
	if s.wrapper, err = createContainer(); err != nil {
		log.Fatalln(err)
	}

	s.cfg = &kafka.Config{
		Brokers: []string{fmt.Sprintf("localhost:%s", s.wrapper.hostPort)},
		Consumer: kafka.ConsumerConfig{
			GroupID: "example-group",
			Topic:   "example-topic",
		},
	}
}

func (s *IntegrationTestSuite) TearDownSuite() {
	pool, container := s.wrapper.pool, s.wrapper.container

	if err := pool.Purge(container); err != nil {
		log.Fatalf("Could not purge container: %s", err)
	}
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

func createContainer() (*DockerTestWrapper, error) {
	hostPort, err := getFreePort()
	if err != nil {
		return nil, fmt.Errorf("could not get free hostPort: %w", err)
	}

	hostPortStr := strconv.Itoa(hostPort)

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("could not connect to docker: %w", err)
	}

	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "docker.vectorized.io/vectorized/redpanda",
		Tag:        "v21.8.1",
		Cmd: []string{
			"redpanda",
			"start",
			"--smp 1",
			"--reserve-memory 0M",
			"--overprovisioned",
			"--node-id 0",
			"--set redpanda.auto_create_topics_enabled=true",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", hostPort),
		},
		ExposedPorts: []string{
			"9092/tcp",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "localhost", HostPort: hostPortStr}},
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true // set AutoRemove to true so that stopped container goes away by itself
	})
	if err != nil {
		return nil, fmt.Errorf("could not start container: %w", err)
	}

	pool.MaxWait = 30 * time.Second

	if err = pool.Retry(func() error {
		err := kafka.CheckHealth(hostPortStr)
		if err != nil {
			fmt.Printf("kafka connection not ready: %v \n", err)
		}
		return err
	}); err != nil {
		return nil, fmt.Errorf("could not retry the pool: %w", err)
	}

	return &DockerTestWrapper{pool: pool, container: container, hostPort: hostPortStr}, nil
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

type DockerTestWrapper struct {
	container *dockertest.Resource
	pool      *dockertest.Pool
	hostPort  string
}

func (w *DockerTestWrapper) SendContainerLogsToStdout() {
	go func() {
		w.pool.Client.Logs(docker.LogsOptions{
			Context:      context.Background(),
			Container:    w.container.Container.ID,
			OutputStream: os.Stdout,
			ErrorStream:  os.Stderr,
			Follow:       true,
			Stdout:       true,
			Stderr:       true,
			Timestamps:   true,
			RawTerminal:  true,
		})
	}()
}
