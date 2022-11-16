package testcontainer

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"net"
	"time"
)

type TestContainerWrapper struct {
	container testcontainers.Container
	hostPort  int
}

func (t *TestContainerWrapper) RunContainer() error {
	hostPort, err := getFreePort()
	if err != nil {
		return fmt.Errorf("could not get free hostPort: %w", err)
	}

	req := testcontainers.ContainerRequest{
		Image: fmt.Sprintf("%s:%s", RedpandaImage, RedpandaVersion),
		ExposedPorts: []string{
			fmt.Sprintf("%d:%d/tcp", hostPort, hostPort),
		},
		Cmd: []string{
			"redpanda",
			"start",
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
