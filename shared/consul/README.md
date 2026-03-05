# Consul

Shared Consul client used by all services for registration and service discovery.

## Usage

### Register on startup

```go
client, err := consul.Register("consul:8500", consul.Config{
    ServiceName: "user-service",
    ServiceID:   "user-service-1",
    Host:        "user-service", // Docker service name
    Port:        8080,
    HealthPath:  "/health",      // optional, defaults to /health
})
if err != nil {
    log.Fatal(err)
}
```

### Deregister on shutdown

```go
defer func() {
    if err := consul.Deregister(client, "user-service-1"); err != nil {
        log.Printf("warn: %v", err)
    }
}()
```

### Discover another service

```go
url, err := consul.Discover(ctx, client, "order-service")
if err != nil {
    return err
}
// url = "http://10.0.0.2:8081"
```

`Discover` selects a healthy instance using **round-robin** load balancing. It falls back to the node address if the service address is empty, and returns an error if no healthy instances are found.

## Config fields

| Field         | Description                                              |
|---------------|----------------------------------------------------------|
| `ServiceName` | Logical name used for discovery (e.g. `user-service`)   |
| `ServiceID`   | Unique instance ID (e.g. `user-service-1`)               |
| `Host`        | Hostname/IP reachable by other services (Docker name)    |
| `Port`        | Port the service listens on                              |
| `HealthPath`  | HTTP health check path. Defaults to `/health`            |

## Running tests

```bash
go test ./consul/...
```

No real Consul instance is required — tests use an `httptest.Server` to mock the Consul HTTP API.
