package functions

import (
	"context"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
)

// UpgradeInfo.upgradeState enum values (Cloud Functions v2). Only the states the
// emulator's 1st→2nd gen control plane can reach are named; the real enum is
// larger and its remaining error states are not modelled.
const (
	// UpgradeStateEligible marks a 1st gen function eligible for a 2nd gen
	// upgrade. It is never set by the emulator (every emulated function is GEN_2)
	// but is accepted by setupFunctionUpgradeConfig so a client that observed a
	// real eligible function can still drive the flow.
	UpgradeStateEligible = "ELIGIBLE_FOR_2ND_GEN_UPGRADE"
	// UpgradeStateSetupSuccessful is set by setupFunctionUpgradeConfig once the
	// 2nd gen copy's configuration has been created; traffic still serves the
	// 1st gen function.
	UpgradeStateSetupSuccessful = "SETUP_FUNCTION_UPGRADE_CONFIG_SUCCESSFUL"
	// UpgradeStateRedirectSuccessful is set by redirectFunctionUpgradeTraffic
	// once traffic has been moved to the 2nd gen copy.
	UpgradeStateRedirectSuccessful = "REDIRECT_FUNCTION_UPGRADE_TRAFFIC_SUCCESSFUL"
	// UpgradeStateCommitAsGen2Successful is set by commitFunctionUpgradeAsGen2
	// once the upgrade is final and the 1st gen function is removed.
	UpgradeStateCommitAsGen2Successful = "COMMIT_FUNCTION_UPGRADE_AS_GEN2_SUCCESSFUL"
)

// UpgradeConfig carries the optional overrides accepted by
// setupFunctionUpgradeConfig (buildConfigOverrides.runtime and
// serviceConfigOverrides.maxInstanceCount). Zero values mean "unset".
type UpgradeConfig struct {
	Runtime          string
	MaxInstanceCount int
}

// UpgradeInputFromMap extracts an UpgradeConfig from a Discovery-shaped
// setupFunctionUpgradeConfig body. The overrides are nested under buildConfig
// and serviceConfig; a missing/empty body yields a zero config.
func UpgradeInputFromMap(body map[string]any) UpgradeConfig {
	var cfg UpgradeConfig
	if bc := nestedMap(body, "buildConfigOverrides"); bc != nil {
		cfg.Runtime = bodyString(bc, "runtime")
	}
	if sc := nestedMap(body, "serviceConfigOverrides"); sc != nil {
		if n, ok := sc["maxInstanceCount"].(float64); ok && n > 0 {
			cfg.MaxInstanceCount = int(n)
		}
	}
	return cfg
}

// upgradeTransition validates the current upgrade state against the set of
// states from which the requested method is allowed to run, returning a
// FailedPrecondition (HTTP 400, gRPC FAILED_PRECONDITION) when it is not. An
// empty allowed list means "any state".
func upgradeTransition(action string, f functionsstore.Function, allowed ...string) error {
	if len(allowed) == 0 {
		return nil
	}
	for _, a := range allowed {
		if f.UpgradeState == a {
			return nil
		}
	}
	return model.NewProviderError("FailedPrecondition",
		"cannot "+action+": function upgrade state is "+displayUpgradeState(f.UpgradeState), 400)
}

// displayUpgradeState renders an empty state for the error message.
func displayUpgradeState(s string) string {
	if s == "" {
		return "UPGRADE_STATE_UNSPECIFIED"
	}
	return s
}

// applyUpgrade runs mutate against the stored function under the store's atomic
// update and returns the persisted function plus its operation (done inline by
// default, in flight under async LRO timing). verb names the upgrade method
// (recorded as the operation's verb/target metadata).
func (s *Service) applyUpgrade(ctx context.Context, project, location, id, verb string, mutate func(functionsstore.Function) (functionsstore.Function, error)) (Operation, error) {
	if location == "" || id == "" {
		return Operation{}, invalidArgument("missing location or function name")
	}
	// UpdateFunctionAtomic returns ErrNoSuchFunction for a missing function,
	// which mapErr below turns into NotFound.
	f, err := s.functions.UpdateFunctionAtomic(ctx, project, location, id, mutate)
	if err != nil {
		return Operation{}, mapErr(err)
	}
	target := resourceID(project)("cloud-function", location+"/"+id)
	op := s.newOperation(location, verb, target, &f)
	if err := s.persistOperation(ctx, project, op); err != nil {
		return Operation{}, err
	}
	return op, nil
}

// SetupFunctionUpgradeConfig creates the 2nd gen copy's configuration from the
// 1st gen function (the first step of the gen1→gen2 upgrade). Traffic continues
// to be served by the 1st gen function until redirectFunctionUpgradeTraffic.
func (s *Service) SetupFunctionUpgradeConfig(ctx context.Context, project, location, id string, cfg UpgradeConfig, _ Version) (Operation, error) {
	return s.applyUpgrade(ctx, project, location, id, "setupFunctionUpgradeConfig", func(f functionsstore.Function) (functionsstore.Function, error) {
		if err := upgradeTransition("setupFunctionUpgradeConfig", f, "", UpgradeStateEligible, UpgradeStateSetupSuccessful); err != nil {
			return functionsstore.Function{}, err
		}
		if cfg.Runtime != "" {
			f.UpgradeRuntime = cfg.Runtime
		}
		if cfg.MaxInstanceCount > 0 {
			f.UpgradeMaxInstances = cfg.MaxInstanceCount
		}
		f.UpgradeState = UpgradeStateSetupSuccessful
		f.UpdateTime = now()
		return f, nil
	})
}

// RedirectFunctionUpgradeTraffic moves traffic from the 1st gen function to the
// 2nd gen copy (the second step). allTrafficOnLatestRevision then renders false
// for the 1st gen function.
func (s *Service) RedirectFunctionUpgradeTraffic(ctx context.Context, project, location, id string, _ Version) (Operation, error) {
	return s.applyUpgrade(ctx, project, location, id, "redirectFunctionUpgradeTraffic", func(f functionsstore.Function) (functionsstore.Function, error) {
		if err := upgradeTransition("redirectFunctionUpgradeTraffic", f, UpgradeStateSetupSuccessful, UpgradeStateRedirectSuccessful); err != nil {
			return functionsstore.Function{}, err
		}
		f.UpgradeState = UpgradeStateRedirectSuccessful
		f.UpgradeTrafficGen2 = true
		f.UpdateTime = now()
		return f, nil
	})
}

// RollbackFunctionUpgradeTraffic reverts traffic from the 2nd gen copy back to
// the 1st gen function.
func (s *Service) RollbackFunctionUpgradeTraffic(ctx context.Context, project, location, id string, _ Version) (Operation, error) {
	return s.applyUpgrade(ctx, project, location, id, "rollbackFunctionUpgradeTraffic", func(f functionsstore.Function) (functionsstore.Function, error) {
		if err := upgradeTransition("rollbackFunctionUpgradeTraffic", f, UpgradeStateRedirectSuccessful); err != nil {
			return functionsstore.Function{}, err
		}
		f.UpgradeState = UpgradeStateSetupSuccessful
		f.UpgradeTrafficGen2 = false
		f.UpdateTime = now()
		return f, nil
	})
}

// CommitFunctionUpgrade finalizes the upgrade after traffic has been redirected:
// the 2nd gen configuration becomes the function and the upgrade metadata is
// cleared (the real enum has no success state for the plain commit).
func (s *Service) CommitFunctionUpgrade(ctx context.Context, project, location, id string, _ Version) (Operation, error) {
	return s.applyUpgrade(ctx, project, location, id, "commitFunctionUpgrade", func(f functionsstore.Function) (functionsstore.Function, error) {
		if err := upgradeTransition("commitFunctionUpgrade", f, UpgradeStateRedirectSuccessful); err != nil {
			return functionsstore.Function{}, err
		}
		commitUpgradeConfig(&f)
		f.UpgradeState = ""
		f.UpdateTime = now()
		return f, nil
	})
}

// CommitFunctionUpgradeAsGen2 finalizes a gen1→gen2 upgrade, leaving the 2nd gen
// function active and manageable by the v2 API. The terminal
// COMMIT_FUNCTION_UPGRADE_AS_GEN2_SUCCESSFUL state is retained so upgradeInfo
// reports the outcome.
func (s *Service) CommitFunctionUpgradeAsGen2(ctx context.Context, project, location, id string, _ Version) (Operation, error) {
	return s.applyUpgrade(ctx, project, location, id, "commitFunctionUpgradeAsGen2", func(f functionsstore.Function) (functionsstore.Function, error) {
		if err := upgradeTransition("commitFunctionUpgradeAsGen2", f, UpgradeStateRedirectSuccessful); err != nil {
			return functionsstore.Function{}, err
		}
		commitUpgradeConfig(&f)
		f.UpgradeState = UpgradeStateCommitAsGen2Successful
		f.UpdateTime = now()
		return f, nil
	})
}

// AbortFunctionUpgrade aborts an in-progress upgrade, discarding the 2nd gen
// copy's configuration and returning the function to its pre-upgrade state.
func (s *Service) AbortFunctionUpgrade(ctx context.Context, project, location, id string, _ Version) (Operation, error) {
	return s.applyUpgrade(ctx, project, location, id, "abortFunctionUpgrade", func(f functionsstore.Function) (functionsstore.Function, error) {
		if err := upgradeTransition("abortFunctionUpgrade", f, UpgradeStateSetupSuccessful, UpgradeStateRedirectSuccessful); err != nil {
			return functionsstore.Function{}, err
		}
		f.UpgradeState = ""
		f.UpgradeTrafficGen2 = false
		f.UpgradeRuntime = ""
		f.UpgradeMaxInstances = 0
		f.UpdateTime = now()
		return f, nil
	})
}

// DetachFunction detaches a 2nd gen function to a Cloud Run function. The
// emulator records the detach (no upgrade is in flight afterwards) but does not
// model a distinct Cloud Run resource.
func (s *Service) DetachFunction(ctx context.Context, project, location, id string, _ Version) (Operation, error) {
	return s.applyUpgrade(ctx, project, location, id, "detachFunction", func(f functionsstore.Function) (functionsstore.Function, error) {
		f.UpgradeState = ""
		f.UpgradeTrafficGen2 = false
		f.UpgradeRuntime = ""
		f.UpgradeMaxInstances = 0
		f.UpdateTime = now()
		return f, nil
	})
}

// commitUpgradeConfig folds the 2nd gen overrides into the function's live
// configuration and clears the pending upgrade fields.
func commitUpgradeConfig(f *functionsstore.Function) {
	if f.UpgradeRuntime != "" {
		f.Runtime = f.UpgradeRuntime
	}
	f.UpgradeTrafficGen2 = true
	f.UpgradeRuntime = ""
	f.UpgradeMaxInstances = 0
}
