package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/docker/docker/client"
	"github.com/stretchr/testify/require"
)

func TestStartDockerPostgresWaitsForExistingRunningContainer(t *testing.T) {
	resetDockerClientEnvironment(t)
	originalLogger := logger
	logger = slog.Default()
	t.Cleanup(func() { logger = originalLogger })

	waited := false
	waitUntilReady := func() error {
		waited = true
		return nil
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/_ping"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/containers/json"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"Id":"container-id","Names":["/dbos-db"],"State":"running"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv(client.EnvOverrideHost, "tcp://"+server.Listener.Addr().String())

	require.NoError(t, startDockerPostgresWithWait(waitUntilReady))
	require.True(t, waited)
}

func resetDockerClientEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv(client.EnvOverrideAPIVersion, "")
	t.Setenv(client.EnvTLSVerify, "")
	t.Setenv(client.EnvOverrideCertPath, "")
}

func TestNewDockerClientUsesExplicitDockerHost(t *testing.T) {
	resetDockerClientEnvironment(t)
	t.Setenv(client.EnvOverrideHost, "unix:///tmp/explicit-docker.sock")

	called := false
	originalInspectDockerContext := inspectDockerContext
	inspectDockerContext = func(context.Context) ([]byte, error) {
		called = true
		return nil, errors.New("active context should not be inspected")
	}
	t.Cleanup(func() { inspectDockerContext = originalInspectDockerContext })

	dockerClient, err := newDockerClient()
	require.NoError(t, err)
	t.Cleanup(func() { dockerClient.Close() })

	require.Equal(t, "unix:///tmp/explicit-docker.sock", dockerClient.DaemonHost())
	require.False(t, called)
}

func TestNewDockerClientUsesActiveDockerContext(t *testing.T) {
	resetDockerClientEnvironment(t)
	t.Setenv(client.EnvOverrideHost, "")
	activeHost := "unix:///Users/test/.colima/default/docker.sock"

	called := false
	originalInspectDockerContext := inspectDockerContext
	inspectDockerContext = func(context.Context) ([]byte, error) {
		called = true
		return []byte("  " + activeHost + "\n"), nil
	}
	t.Cleanup(func() { inspectDockerContext = originalInspectDockerContext })

	dockerClient, err := newDockerClient()
	require.NoError(t, err)
	t.Cleanup(func() { dockerClient.Close() })

	require.Equal(t, activeHost, dockerClient.DaemonHost())
	require.True(t, called)
}

func TestNewDockerClientRejectsMissingActiveDockerContextEndpoint(t *testing.T) {
	resetDockerClientEnvironment(t)
	t.Setenv(client.EnvOverrideHost, "")

	originalInspectDockerContext := inspectDockerContext
	inspectDockerContext = func(context.Context) ([]byte, error) {
		return []byte("<no value>\n"), nil
	}
	t.Cleanup(func() { inspectDockerContext = originalInspectDockerContext })

	_, err := newDockerClient()
	require.EqualError(t, err, "active Docker context has no Docker endpoint")
}

func TestNewDockerClientReportsContextInspectionFailure(t *testing.T) {
	resetDockerClientEnvironment(t)
	t.Setenv(client.EnvOverrideHost, "")

	originalInspectDockerContext := inspectDockerContext
	inspectDockerContext = func(context.Context) ([]byte, error) {
		return nil, errors.New("docker CLI unavailable")
	}
	t.Cleanup(func() { inspectDockerContext = originalInspectDockerContext })

	_, err := newDockerClient()
	require.EqualError(t, err, "failed to inspect active Docker context: docker CLI unavailable")
}
