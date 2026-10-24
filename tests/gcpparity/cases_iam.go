//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	iam "cloud.google.com/go/iam/apiv1"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	pubsubapiv1 "cloud.google.com/go/pubsub/v2/apiv1"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// iamScenario compares the shared google.iam.v1.IAMPolicy surface over REST and
// gRPC. The emulator's dual "iam" service is asymmetric: the REST provider is
// service-account admin (projects.serviceAccounts, whose IAM policy is only
// reachable over REST), while the gRPC side is the shared IAMPolicy service
// routed across Pub/Sub, KMS, Eventarc and Metastore. They own no common
// resource of their own, so a like-for-like comparison needs a shared policy
// target — exactly one the exemption in testdata/parity-exemptions.json named.
//
// The scenario uses run-unique fixtures as the targets and drives
// GetIamPolicy / SetIamPolicy / TestIamPermissions over both transports against
// each. Both paths reach one shared policy store (internal/gcp/policy), so this
// asserts the REST Discovery envelope and the gRPC protojson rendering of the
// same Policy agree — and that a write over either transport is visible on the
// other. AUD3-6 introduced the scenario with a Pub/Sub topic target; AUD3-16
// extends it to every handler the router dispatches to: the Pub/Sub
// subscription policy resource type and the KMS, Eventarc and Metastore
// handlers. Each owning handler has its own REST route and its own
// Policy→proto renderer, so a divergence in any one would otherwise be
// invisible to the cross-transport gate.
func iamScenario() Scenario {
	const (
		topicID    = "iam-policy"
		subID      = "iam-policy-sub"
		subTopicID = "iam-policy-sub-topic"
		ringID     = "iam-policy-ring"
		keyID      = "iam-policy-key"
		triggerID  = "iam-policy-trigger"
		serviceID  = "iam-policy-service"
		kmsLoc     = "global"
		region     = "us-central1"
	)

	topicName := func(e *Env) string {
		return fmt.Sprintf("projects/%s/topics/%s", e.Cfg.Project, e.Resource(topicID))
	}
	subTopicName := func(e *Env) string {
		return fmt.Sprintf("projects/%s/topics/%s", e.Cfg.Project, e.Resource(subTopicID))
	}
	subName := func(e *Env) string {
		return fmt.Sprintf("projects/%s/subscriptions/%s", e.Cfg.Project, e.Resource(subID))
	}
	kmsParent := func(e *Env) string {
		return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, kmsLoc)
	}
	keyRingName := func(e *Env) string { return kmsParent(e) + "/keyRings/" + e.Resource(ringID) }
	cryptoKeyName := func(e *Env) string { return keyRingName(e) + "/cryptoKeys/" + e.Resource(keyID) }
	triggerName := func(e *Env) string {
		return fmt.Sprintf("projects/%s/locations/%s/triggers/%s", e.Cfg.Project, region, e.Resource(triggerID))
	}
	serviceName := func(e *Env) string {
		return fmt.Sprintf("projects/%s/locations/%s/services/%s", e.Cfg.Project, region, e.Resource(serviceID))
	}

	// The two transports must carry the same logical binding so the diff compares
	// like against like; only the encoding differs (REST JSON vs a typed Policy).
	// Role strings are not authorized in the emulator, so one binding serves every
	// target.
	const role = "roles/viewer"
	members := []string{"user:parity@example.com"}

	// ─── fixtures ─────────────────────────────────────────────────────────────
	// The topic is created over gRPC (the same client the subscription fixture
	// uses); every other fixture is created over REST, so the scenario exercises
	// both transports as the writer as well as the reader.

	createTopic := func(ctx context.Context, e *Env) error {
		c, err := pubsubapiv1.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName(e)})
		return err
	}
	deleteTopic := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v1/"+topicName(e)) }

	createSubscription := func(ctx context.Context, e *Env) error {
		tc, err := pubsubapiv1.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer tc.Close()
		if _, err := tc.CreateTopic(ctx, &pubsubpb.Topic{Name: subTopicName(e)}); err != nil {
			return err
		}
		sc, err := pubsubapiv1.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer sc.Close()
		_, err = sc.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name: subName(e), Topic: subTopicName(e), AckDeadlineSeconds: 10,
		})
		return err
	}
	deleteSubscription := func(ctx context.Context, e *Env) error {
		if err := e.RestDelete(ctx, "/v1/"+subName(e)); err != nil {
			return err
		}
		return e.RestDelete(ctx, "/v1/"+subTopicName(e))
	}

	// Key rings and crypto keys cannot be deleted by the real API (a key is only
	// scheduled for destruction), so their twins are left in place.
	createKeyRing := func(ctx context.Context, e *Env) error {
		_, err := e.Rest(ctx, http.MethodPost, "/v1/"+kmsParent(e)+"/keyRings?keyRingId="+e.Resource(ringID), "{}")
		return err
	}
	createCryptoKey := func(ctx context.Context, e *Env) error {
		_, err := e.Rest(ctx, http.MethodPost, "/v1/"+keyRingName(e)+"/cryptoKeys?cryptoKeyId="+e.Resource(keyID),
			`{"purpose":"ENCRYPT_DECRYPT"}`)
		return err
	}
	// createCryptoKeyFixture creates the owning key ring too, for the
	// mutation-parity step whose twins have no shared key ring.
	createCryptoKeyFixture := func(ctx context.Context, e *Env) error {
		if err := createKeyRing(ctx, e); err != nil {
			return err
		}
		return createCryptoKey(ctx, e)
	}

	// triggerBody is the minimal valid Eventarc trigger (the same shape
	// cases_eventarc.go creates): an HTTP destination plus a type/bucket filter.
	const triggerBody = `{"destination":{"httpEndpoint":{"uri":"https://example.com/parity"}},` +
		`"eventFilters":[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},` +
		`{"attribute":"bucket","value":"parity-bucket"}]}`
	createTrigger := func(ctx context.Context, e *Env) error {
		_, err := e.RestOperationResource(ctx, http.MethodPost,
			"/v1/projects/"+e.Cfg.Project+"/locations/"+region+"/triggers?triggerId="+e.Resource(triggerID), triggerBody)
		return err
	}
	deleteTrigger := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v1/"+triggerName(e)) }

	createService := func(ctx context.Context, e *Env) error {
		_, err := e.RestOperationResource(ctx, http.MethodPost,
			"/v1/projects/"+e.Cfg.Project+"/locations/"+region+"/services?serviceId="+e.Resource(serviceID),
			`{"labels":{"parity":"true"}}`)
		return err
	}
	deleteService := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v1/"+serviceName(e)) }

	// ─── targets ──────────────────────────────────────────────────────────────
	topic := newIamTarget(topicName, func(e *Env) string { return "/v1/" + topicName(e) },
		role, members, []string{"pubsub.topics.get", "pubsub.topics.publish"})
	subscription := newIamTarget(subName, func(e *Env) string { return "/v1/" + subName(e) },
		role, members, []string{"pubsub.subscriptions.get", "pubsub.subscriptions.consume"})
	keyRing := newIamTarget(keyRingName, func(e *Env) string { return "/v1/" + keyRingName(e) },
		role, members, []string{"cloudkms.keyRings.get"})
	cryptoKey := newIamTarget(cryptoKeyName, func(e *Env) string { return "/v1/" + cryptoKeyName(e) },
		role, members, []string{"cloudkms.cryptoKeys.get"})
	trigger := newIamTarget(triggerName, func(e *Env) string { return "/v1/" + triggerName(e) },
		role, members, []string{"eventarc.triggers.get"})
	service := newIamTarget(serviceName, func(e *Env) string { return "/v1/" + serviceName(e) },
		role, members, []string{"metastore.services.get"})

	steps := []Step{
		// Pub/Sub topic (AUD3-6): the topic is written over REST and read back
		// over both transports, so the REST write is proven visible to the gRPC
		// service (one shared store).
		{Op: "CreateTopic", Mutate: createTopic},
	}
	steps = append(steps, topic.readSteps("Topic")...)
	steps = append(steps, topic.mutationStep("Topic", createTopic, deleteTopic))
	steps = append(steps, Step{Op: "DeleteTopic", Mutate: deleteTopic})

	// Pub/Sub subscription policy resource type (AUD3-16).
	steps = append(steps,
		Step{Op: "CreateSubscription", Mutate: createSubscription})
	steps = append(steps, subscription.readSteps("Subscription")...)
	steps = append(steps, subscription.mutationStep("Subscription", createSubscription, deleteSubscription))
	steps = append(steps, Step{Op: "DeleteSubscription", Mutate: deleteSubscription})

	// KMS key-ring and crypto-key handlers (AUD3-16). No delete: the real API
	// cannot remove either, so the fixtures and their twins persist.
	steps = append(steps, Step{Op: "CreateKeyRing", Mutate: createKeyRing})
	steps = append(steps, keyRing.readSteps("KeyRing")...)
	steps = append(steps, keyRing.mutationStep("KeyRing", createKeyRing, nil))
	steps = append(steps, Step{Op: "CreateCryptoKey", Mutate: createCryptoKey})
	steps = append(steps, cryptoKey.readSteps("CryptoKey")...)
	steps = append(steps, cryptoKey.mutationStep("CryptoKey", createCryptoKeyFixture, nil))

	// Eventarc trigger handler (AUD3-16).
	steps = append(steps, Step{Op: "CreateTrigger", Mutate: createTrigger})
	steps = append(steps, trigger.readSteps("Trigger")...)
	steps = append(steps, trigger.mutationStep("Trigger", createTrigger, deleteTrigger))
	steps = append(steps, Step{Op: "DeleteTrigger", Mutate: deleteTrigger})

	// Dataproc Metastore service handler (AUD3-16).
	steps = append(steps, Step{Op: "CreateService", Mutate: createService})
	steps = append(steps, service.readSteps("Service")...)
	steps = append(steps, service.mutationStep("Service", createService, deleteService))
	steps = append(steps, Step{Op: "DeleteService", Mutate: deleteService})

	return Scenario{Service: "iam", Steps: steps}
}

// iamTarget bundles the cross-transport accessors for one IAM policy target: the
// gRPC resource name and the REST custom-method path. Each REST call posts to
// the Discovery `:getIamPolicy` / `:setIamPolicy` / `:testIamPermissions` verb on
// the target resource.
type iamTarget struct {
	resource    func(*Env) string
	restPath    func(*Env) string
	role        string
	members     []string
	permissions []string
}

// newIamTarget returns the accessor set for one policy target.
func newIamTarget(resource, restPath func(*Env) string, role string, members, permissions []string) iamTarget {
	return iamTarget{resource: resource, restPath: restPath, role: role, members: members, permissions: permissions}
}

func (t iamTarget) client(ctx context.Context, e *Env) (*iam.IamPolicyClient, error) {
	return iam.NewIamPolicyClient(ctx, e.GRPCClientOptions()...)
}

// getGRPC reads the target's policy over gRPC.
func (t iamTarget) getGRPC(ctx context.Context, e *Env) (protoMessage, error) {
	c, err := t.client(ctx, e)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: t.resource(e)})
}

// getREST reads the same target's policy over REST. getIamPolicy is a POST
// custom method with an optional request body (here empty options).
func (t iamTarget) getREST(ctx context.Context, e *Env) (json.RawMessage, error) {
	return e.Rest(ctx, http.MethodPost, t.restPath(e)+":getIamPolicy", "{}")
}

func (t iamTarget) setGRPC(ctx context.Context, e *Env) (protoMessage, error) {
	c, err := t.client(ctx, e)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: t.resource(e),
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: t.role, Members: t.members}}},
	})
}

func (t iamTarget) setREST(ctx context.Context, e *Env) (json.RawMessage, error) {
	body, _ := json.Marshal(map[string]any{
		"policy": map[string]any{
			"bindings": []any{map[string]any{"role": t.role, "members": t.members}},
		},
	})
	return e.Rest(ctx, http.MethodPost, t.restPath(e)+":setIamPolicy", string(body))
}

// setGRPCCanonical / setRESTCanonical are the Mutate-step forms (the response is
// discarded); the following read step re-reads the policy over both transports.
func (t iamTarget) setGRPCCanonical(ctx context.Context, e *Env) error {
	_, err := t.setGRPC(ctx, e)
	return err
}

func (t iamTarget) setRESTCanonical(ctx context.Context, e *Env) error {
	_, err := t.setREST(ctx, e)
	return err
}

func (t iamTarget) testGRPC(ctx context.Context, e *Env) (protoMessage, error) {
	c, err := t.client(ctx, e)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    t.resource(e),
		Permissions: t.permissions,
	})
}

func (t iamTarget) testREST(ctx context.Context, e *Env) (json.RawMessage, error) {
	body, _ := json.Marshal(map[string]any{"permissions": t.permissions})
	return e.Rest(ctx, http.MethodPost, t.restPath(e)+":testIamPermissions", string(body))
}

// readSteps returns the canonical cross-transport reads for one target: the
// default policy, a write over REST read back over both transports, a write
// over gRPC read back over both, and a permissions probe.
func (t iamTarget) readSteps(label string) []Step {
	return []Step{
		{Op: label + " GetIamPolicy (default)", GRPC: t.getGRPC, REST: t.getREST},
		{Op: label + " SetIamPolicy (REST)", Mutate: t.setRESTCanonical, GRPC: t.getGRPC, REST: t.getREST},
		{Op: label + " SetIamPolicy (gRPC)", Mutate: t.setGRPCCanonical, GRPC: t.getGRPC, REST: t.getREST},
		{Op: label + " TestIamPermissions", GRPC: t.testGRPC, REST: t.testREST},
	}
}

// mutationStep cross-diffs one SetIamPolicy response across transports: each
// transport creates its own twin fixture (through create) and writes the same
// logical policy, and the two Policy bodies are compared. The Policy response
// carries no resource name, so no projection is needed.
func (t iamTarget) mutationStep(label string, create func(context.Context, *Env) error, cleanup func(context.Context, *Env) error) Step {
	return Step{
		Op: label + " SetIamPolicy (parity)",
		Mutation: &MutationParity{
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				if err := create(ctx, e); err != nil {
					return nil, err
				}
				return t.setGRPC(ctx, e)
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				if err := create(ctx, e); err != nil {
					return nil, err
				}
				return t.setREST(ctx, e)
			},
			Cleanup: cleanup,
		},
	}
}
