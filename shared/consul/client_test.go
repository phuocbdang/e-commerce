package consul

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	consulapi "github.com/hashicorp/consul/api"
)

// newTestClient points a Consul client at a local httptest server.
func newTestClient(t *testing.T, server *httptest.Server) *consulapi.Client {
	t.Helper()
	cfg := consulapi.DefaultConfig()
	cfg.Address = strings.TrimPrefix(server.URL, "http://")
	client, err := consulapi.NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create test consul client: %v", err)
	}
	return client
}

func TestNewClient(t *testing.T) {
	t.Run("uses provided address", func(t *testing.T) {
		client, err := NewClient("consul:8500")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if client == nil {
			t.Fatal("expected non-nil client")
		}
	})

	t.Run("uses default address when empty", func(t *testing.T) {
		client, err := NewClient("")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if client == nil {
			t.Fatal("expected non-nil client")
		}
	})
}

func TestRegister(t *testing.T) {
	t.Run("registers with default health path", func(t *testing.T) {
		var gotBody consulapi.AgentServiceRegistration
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && r.URL.Path == "/v1/agent/service/register" {
				if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				w.WriteHeader(http.StatusOK)
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		addr := strings.TrimPrefix(srv.URL, "http://")
		_, err := Register(addr, Config{
			ServiceName: "test-svc",
			ServiceID:   "test-svc-1",
			Host:        "test-svc",
			Port:        8080,
		})
		if err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if !strings.HasSuffix(gotBody.Check.HTTP, defaultHealthPath) {
			t.Errorf("expected health path %q, got %q", defaultHealthPath, gotBody.Check.HTTP)
		}
	})

	t.Run("registers with custom health path", func(t *testing.T) {
		var gotBody consulapi.AgentServiceRegistration
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && r.URL.Path == "/v1/agent/service/register" {
				json.NewDecoder(r.Body).Decode(&gotBody)
				w.WriteHeader(http.StatusOK)
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		addr := strings.TrimPrefix(srv.URL, "http://")
		_, err := Register(addr, Config{
			ServiceName: "test-svc",
			ServiceID:   "test-svc-1",
			Host:        "test-svc",
			Port:        8080,
			HealthPath:  "/ping",
		})
		if err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if !strings.HasSuffix(gotBody.Check.HTTP, "/ping") {
			t.Errorf("expected health path /ping, got %q", gotBody.Check.HTTP)
		}
	})

	t.Run("returns error when consul is unavailable", func(t *testing.T) {
		_, err := Register("127.0.0.1:1", Config{
			ServiceName: "test-svc",
			ServiceID:   "test-svc-1",
			Host:        "test-svc",
			Port:        8080,
		})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestDeregister(t *testing.T) {
	t.Run("deregisters successfully", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/agent/service/deregister/") {
				w.WriteHeader(http.StatusOK)
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		client := newTestClient(t, srv)
		if err := Deregister(client, "test-svc-1"); err != nil {
			t.Fatalf("Deregister returned error: %v", err)
		}
	})

	t.Run("returns error on failure", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}))
		defer srv.Close()

		client := newTestClient(t, srv)
		if err := Deregister(client, "test-svc-1"); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestDiscover(t *testing.T) {
	makeEntry := func(serviceAddr, nodeAddr string, port int) consulapi.ServiceEntry {
		return consulapi.ServiceEntry{
			Node:    &consulapi.Node{Address: nodeAddr},
			Service: &consulapi.AgentService{Address: serviceAddr, Port: port},
		}
	}

	newMockServer := func(entries []consulapi.ServiceEntry) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/v1/health/service/") {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Consul-Index", "1")
				json.NewEncoder(w).Encode(entries)
				return
			}
			http.NotFound(w, r)
		}))
	}

	t.Run("returns service URL from service address", func(t *testing.T) {
		rrCounter.Store(0)
		srv := newMockServer([]consulapi.ServiceEntry{
			makeEntry("10.0.0.1", "", 8080),
		})
		defer srv.Close()

		client := newTestClient(t, srv)
		url, err := Discover(context.Background(), client, "my-svc")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if url != "http://10.0.0.1:8080" {
			t.Errorf("expected http://10.0.0.1:8080, got %q", url)
		}
	})

	t.Run("falls back to node address when service address is empty", func(t *testing.T) {
		rrCounter.Store(0)
		srv := newMockServer([]consulapi.ServiceEntry{
			makeEntry("", "10.0.0.2", 9090),
		})
		defer srv.Close()

		client := newTestClient(t, srv)
		url, err := Discover(context.Background(), client, "my-svc")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if url != "http://10.0.0.2:9090" {
			t.Errorf("expected http://10.0.0.2:9090, got %q", url)
		}
	})

	t.Run("returns error when both addresses are empty", func(t *testing.T) {
		rrCounter.Store(0)
		srv := newMockServer([]consulapi.ServiceEntry{
			makeEntry("", "", 8080),
		})
		defer srv.Close()

		client := newTestClient(t, srv)
		_, err := Discover(context.Background(), client, "my-svc")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error when no healthy instances", func(t *testing.T) {
		rrCounter.Store(0)
		srv := newMockServer([]consulapi.ServiceEntry{})
		defer srv.Close()

		client := newTestClient(t, srv)
		_, err := Discover(context.Background(), client, "my-svc")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("round-robin distributes across instances", func(t *testing.T) {
		rrCounter.Store(0)
		srv := newMockServer([]consulapi.ServiceEntry{
			makeEntry("10.0.0.1", "", 8080),
			makeEntry("10.0.0.2", "", 8080),
			makeEntry("10.0.0.3", "", 8080),
		})
		defer srv.Close()

		client := newTestClient(t, srv)
		ctx := context.Background()

		seen := map[string]int{}
		for i := range 9 {
			url, err := Discover(ctx, client, "my-svc")
			if err != nil {
				t.Fatalf("call %d: unexpected error: %v", i, err)
			}
			seen[url]++
		}

		if len(seen) != 3 {
			t.Errorf("expected 3 distinct instances, got %d: %v", len(seen), seen)
		}
		for url, count := range seen {
			if count != 3 {
				t.Errorf("instance %q called %d times, expected 3", url, count)
			}
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		srv := newMockServer([]consulapi.ServiceEntry{})
		defer srv.Close()

		client := newTestClient(t, srv)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := Discover(ctx, client, "my-svc")
		if err == nil {
			t.Fatal("expected error for cancelled context, got nil")
		}
	})
}
