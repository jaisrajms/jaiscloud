package main

import (
	"context"
	"errors"
	"testing"
)

// TestEffectiveLambdaMode locks the docker-mode daemon probe: a docker mode
// with no reachable daemon degrades to mock (as GCP Cloud Run/Dataproc do),
// while every other mode passes through untouched and never probes.
func TestEffectiveLambdaMode(t *testing.T) {
	reachable := func(context.Context) error { return nil }
	unreachable := func(context.Context) error {
		return errors.New("dial unix /var/run/docker.sock: connect: no such file or directory")
	}

	cases := []struct {
		name string
		mode string
		ping func(context.Context) error
		want string
	}{
		{"mock passes through", "mock", reachable, "mock"},
		{"empty passes through", "", reachable, ""},
		{"k8s passes through without probing", "k8s", unreachable, "k8s"},
		{"docker with a reachable daemon stays docker", "docker", reachable, "docker"},
		{"docker with no daemon degrades to mock", "docker", unreachable, "mock"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveLambdaMode(tc.mode, tc.ping); got != tc.want {
				t.Fatalf("effectiveLambdaMode(%q) = %q, want %q", tc.mode, got, tc.want)
			}
		})
	}
}
