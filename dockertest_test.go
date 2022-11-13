package integration_tests_with_redpanda

import (
	"context"
	"fmt"
	"github.com/Abdulsametleri/integration-tests-with-redpanda/kafka"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"log"
	"os"
	"strconv"
	"time"
)

// Implement interface
var _ IntegrationLibraryStrategy = (*DockerTestStrategy)(nil)

type DockerTestStrategy struct {
	container *dockertest.Resource
	pool      *dockertest.Pool
	hostPort  int
}

func (l *DockerTestStrategy) RunContainer() error {
	hostPort, err := getFreePort()
	if err != nil {
		return fmt.Errorf("could not get free hostPort: %w", err)
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		return fmt.Errorf("could not connect to docker: %w", err)
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
			"9092/tcp": {{HostIP: "localhost", HostPort: strconv.Itoa(hostPort)}},
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true // set AutoRemove to true so that stopped container goes away by itself
	})
	if err != nil {
		return fmt.Errorf("could not start container: %w", err)
	}

	pool.MaxWait = 30 * time.Second

	if err = pool.Retry(func() error {
		err := kafka.CheckHealth(hostPort)
		if err != nil {
			fmt.Printf("kafka connection not ready: %v \n", err)
		}
		return err
	}); err != nil {
		return fmt.Errorf("could not retry the pool: %w", err)
	}

	l.pool = pool
	l.container = container
	l.hostPort = hostPort

	return nil
}

func (l *DockerTestStrategy) CleanUp() {
	if err := l.pool.Purge(l.container); err != nil {
		log.Printf("Could not purge container: %s\n", err)
	}
}

func (l *DockerTestStrategy) StreamContainerLogsToStdout() {
	go func() {
		err := l.pool.Client.Logs(docker.LogsOptions{
			Context:      context.Background(),
			Container:    l.container.Container.ID,
			OutputStream: os.Stdout,
			ErrorStream:  os.Stderr,
			Follow:       true,
			Stdout:       true,
			Stderr:       true,
			Timestamps:   true,
			RawTerminal:  true,
		})
		if err != nil {
			log.Printf("Could not stream container logs to stdout: %s", err)
		}
	}()
}

func (l *DockerTestStrategy) GetBrokerAddresses() []string {
	return []string{fmt.Sprintf("localhost:%d", l.hostPort)}
}
