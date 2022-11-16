package testcontainer

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"time"
)

type TestContainerWrapper struct {
	container testcontainers.Container
	hostPort  int
}

func (t *TestContainerWrapper) RunContainer() error {
	req := testcontainers.ContainerRequest{
		Image: fmt.Sprintf("%s:%s", RedpandaImage, RedpandaVersion),
		ExposedPorts: []string{
			"9092:9092/tcp",
		},
		Cmd:        []string{"redpanda", "start"},
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

	mPort, err := container.MappedPort(context.Background(), "9092")
	if err != nil {
		return fmt.Errorf("could not get mapped port from the container: %w", err)
	}

	t.container = container
	t.hostPort = mPort.Int()

	return nil
}

func (t *TestContainerWrapper) CleanUp() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	if err := t.container.Terminate(ctx); err != nil {
		log.Printf("could not terminate container: %s\n", err)
	}
}

func (t *TestContainerWrapper) GetBrokerAddresses() []string {
	return []string{fmt.Sprintf("localhost:%d", t.hostPort)}
}
