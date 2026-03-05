package consul

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"

	consulapi "github.com/hashicorp/consul/api"
)

const defaultHealthPath = "/health"

var rrCounter atomic.Uint64

type Config struct {
	ServiceName string
	ServiceID   string
	Host        string
	Port        int
	HealthPath  string // defaults to /health if empty
}

// NewClient creates a Consul API client pointing at addr (e.g. "consul:8500").
func NewClient(addr string) (*consulapi.Client, error) {
	cfg := consulapi.DefaultConfig()
	if addr != "" {
		cfg.Address = addr
	}
	return consulapi.NewClient(cfg)
}

// Register registers the service with Consul and returns the client.
func Register(addr string, cfg Config) (*consulapi.Client, error) {
	client, err := NewClient(addr)
	if err != nil {
		return nil, err
	}

	healthPath := cfg.HealthPath
	if healthPath == "" {
		healthPath = defaultHealthPath
	}

	reg := &consulapi.AgentServiceRegistration{
		ID:      cfg.ServiceID,
		Name:    cfg.ServiceName,
		Address: cfg.Host,
		Port:    cfg.Port,
		Check: &consulapi.AgentServiceCheck{
			HTTP:                           fmt.Sprintf("http://%s:%d%s", cfg.Host, cfg.Port, healthPath),
			Interval:                       "10s",
			Timeout:                        "5s",
			DeregisterCriticalServiceAfter: "30s",
		},
	}

	if err := client.Agent().ServiceRegister(reg); err != nil {
		return nil, err
	}

	log.Printf("consul: registered %s (%s)", cfg.ServiceName, cfg.ServiceID)
	return client, nil
}

// Deregister removes the service from Consul.
func Deregister(client *consulapi.Client, serviceID string) error {
	if err := client.Agent().ServiceDeregister(serviceID); err != nil {
		return fmt.Errorf("consul: deregister %s: %w", serviceID, err)
	}
	log.Printf("consul: deregistered %s", serviceID)
	return nil
}

// Discover returns the base URL of a healthy service instance using round-robin selection.
func Discover(ctx context.Context, client *consulapi.Client, serviceName string) (string, error) {
	opts := (&consulapi.QueryOptions{}).WithContext(ctx)
	services, _, err := client.Health().Service(serviceName, "", true, opts)
	if err != nil {
		return "", fmt.Errorf("consul discover %s: %w", serviceName, err)
	}
	if len(services) == 0 {
		return "", fmt.Errorf("no healthy instances of %s", serviceName)
	}

	idx := rrCounter.Add(1) - 1
	entry := services[idx%uint64(len(services))]

	host := entry.Service.Address
	if host == "" {
		host = entry.Node.Address
	}
	if host == "" {
		return "", fmt.Errorf("consul discover %s: no address available", serviceName)
	}

	return fmt.Sprintf("http://%s:%d", host, entry.Service.Port), nil
}
