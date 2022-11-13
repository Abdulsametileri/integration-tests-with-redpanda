package integration_tests_with_redpanda

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"time"
)

// Implement interface
var _ IntegrationLibraryStrategy = (*TestContainerStrategy)(nil)

type TestContainerStrategy struct {
	container testcontainers.Container
	hostPort  int
}

func (t *TestContainerStrategy) RunContainer() error {
	hostPort, err := getFreePort()
	if err != nil {
		return fmt.Errorf("could not get free hostPort: %w", err)
	}

	req := testcontainers.ContainerRequest{
		Image: "docker.vectorized.io/vectorized/redpanda:v21.8.1",
		ExposedPorts: []string{
			fmt.Sprintf("%d:%d/tcp", hostPort, hostPort),
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--smp 1",
			"--reserve-memory 0M",
			"--overprovisioned",
			"--node-id 0",
			"--set redpanda.auto_create_topics_enabled=true",
			"--kafka-addr", fmt.Sprintf("0.0.0.0:%d", hostPort),
		},
		WaitingFor: wait.ForLog("Successfully started Redpanda!"),
		AutoRemove: true,
	}

	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return fmt.Errorf("could not create container: %w", err)
	}

	mPort, err := container.MappedPort(context.Background(), nat.Port(fmt.Sprintf("%d", hostPort)))
	if err != nil {
		return fmt.Errorf("could not get mapped port from the container: %w", err)
	}

	t.container = container
	t.hostPort = mPort.Int()

	return nil
}

func (t *TestContainerStrategy) Cleanup() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	if err := t.container.Terminate(ctx); err != nil {
		log.Printf("could not terminate container: %s\n", err)
	}
}

func (t *TestContainerStrategy) GetHostPort() int {
	return t.hostPort
}
