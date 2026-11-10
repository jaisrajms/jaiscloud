package metastore

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"jaiscloud/internal/model"
)

// Service port selection.
//
// A Service's wire `port` is not a cosmetic output on this emulator: the Thrift
// protocol carries no service identity, so the listener a client connects to is
// what selects its catalog (see catalog.go). The API field is Optional
// ("The TCP port at which the metastore service is reached. Default: 9083."),
// and real Metastore echoes a port a caller requests — the Terraform provider
// models it as Optional+Computed (and sends updateMask=port on a change), so
// ignoring the request reads back a different port and permadiffs. The emulator
// therefore honors a requested port on create and update.
//
// Two consequences follow from "the port is the catalog selector":
//
//   - The base --hms-port (9083 by default) stays reserved for the legacy shared
//     catalog, so a Service cannot claim it; the request fails loud instead of
//     advertising an endpoint that would serve another catalog.
//   - Two Services cannot share a port (each needs its own listener), so a
//     duplicate request is AlreadyExists naming the Service that holds it.
//
// Real GCP gives every Service its own endpoint address, so neither constraint
// exists there; both are emulator topology boundaries, recorded in the ledger.
// A Service that requests no port keeps the emulator assignment (base+1, then
// upward, never reused — see nextServicePort).

// requestedPort decodes a caller-supplied Service.port from a create/update
// body. A missing or zero port means "assign one" on create, "no change" on
// update (proto3's zero is an unset field); a non-integer or out-of-range value
// is InvalidArgument.
func requestedPort(cfg json.RawMessage) (int, error) {
	m := decodeConfig(cfg)
	v, ok := m["port"]
	if !ok || v == nil {
		return 0, nil
	}
	f, ok := v.(float64)
	if !ok {
		return 0, invalidArgument("invalid port: must be an integer")
	}
	if f == 0 {
		return 0, nil
	}
	if f != math.Trunc(f) {
		return 0, invalidArgument(fmt.Sprintf("invalid port %v: must be an integer", f))
	}
	if f < 1 || f > 65535 {
		return 0, invalidArgument(fmt.Sprintf("invalid port %v: must be between 1 and 65535", f))
	}
	return int(f), nil
}

// validateServicePort reports whether port can be the requesting Service's
// catalog listener. self is the requesting Service's catalog id ("" on create),
// excluded from the uniqueness check so an update may keep its own port.
func (s *Service) validateServicePort(ctx context.Context, port int, self string) error {
	if port < 1 || port > 65535 {
		return invalidArgument(fmt.Sprintf("invalid port %d: must be between 1 and 65535", port))
	}
	if base := s.baseHMSPort(); port == base {
		return invalidArgument(fmt.Sprintf("port %d is the legacy shared Hive Metastore port (--hms-port); request a different port for this Service", port))
	}
	services, err := s.store.ListAllServices(ctx)
	if err != nil {
		return err
	}
	for _, svc := range services {
		if CatalogID(svc.ProjectID, svc.Location, svc.Name) == self {
			continue
		}
		if svc.Port == port {
			return model.NewProviderError("AlreadyExists", fmt.Sprintf(
				"port %d is already served by %s; each Service needs its own catalog port on this emulator",
				port, ServiceName(svc.ProjectID, svc.Location, svc.Name)), 409)
		}
	}
	return nil
}
