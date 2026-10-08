// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e/fixturecleanup"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

// substrateTemplateSubstitutions is the placeholder set the protojson-shaped
// ActorTemplate fixture templates carry: the substrate counterpart of
// fixtureSubstitutions, whose block fragments are CRD-shaped and now serve
// only the pool manifests. The inline placeholders match the pool manifests'
// values so one (bucket, name) pair renders both halves of a fixture.
func substrateTemplateSubstitutions(bucket, name string, trustBundle bool) (inline, blocks map[string]string) {
	inline = map[string]string{
		"${BUCKET_NAME}":    bucket,
		"${FIXTURE_SUFFIX}": "-" + name,
	}
	blocks = map[string]string{
		// gvisor-default is the cluster-wide SandboxConfig
		// manifests/ate-install ships; config_name is required, so the
		// fixtures name it explicitly.
		"${TEMPLATE_SANDBOX_CONFIG}": "sandboxConfig:\n  sandboxClass: SANDBOX_CLASS_GVISOR\n  configName: gvisor-default",
		"${TEMPLATE_RESOURCES}":      "",
		// Off unless the caller opts in; see WithTrustBundle.
		"${TEMPLATE_TRUST_BUNDLE}": "",
	}
	if trustBundle {
		// Indented to sit in a template's systemInfo dataSources list. The
		// name must be on atelet's supported-bundle allowlist.
		blocks["${TEMPLATE_TRUST_BUNDLE}"] = "    - trustBundle:\n        names:\n        - egress-mitm.ate.dev\n        path: trust-bundle.pem"
	}
	if !IsMicroVM() {
		return inline, blocks
	}

	inline["${FIXTURE_SUFFIX}"] = "-" + SandboxClassMicroVM + "-" + name
	// The cluster-wide SandboxConfig hack/install-microvm-deps.sh installs;
	// a missing or stale one fails loudly at template creation.
	blocks["${TEMPLATE_SANDBOX_CONFIG}"] = "sandboxConfig:\n  sandboxClass: SANDBOX_CLASS_MICROVM\n  configName: microvm"
	// Only for fixtures that declare no limits of their own. Without them the
	// guest boots at ateom's default size (2GiB), and several of those
	// do not fit beside the demo pools on CI's single kind node. These size
	// the VM itself — see internal/sizing. Quantities are strings.
	blocks["${TEMPLATE_RESOURCES}"] = "resources:\n  limits:\n  - name: cpu\n    quantity: \"1\"\n  - name: memory\n    quantity: 512Mi"
	return inline, blocks
}

// decodeSubstrateTemplates strict-decodes a rendered manifest of one or more
// ---separated protojson-shaped ActorTemplate documents. Strict, so a block
// placeholder landing at the wrong depth or a misspelled field fails here
// rather than applying cleanly and doing nothing — the same contract
// `kubectl ate create actor-template` enforces.
func decodeSubstrateTemplates(t *testing.T, rendered []byte) []*ateapipb.ActorTemplate {
	t.Helper()
	var templates []*ateapipb.ActorTemplate
	for doc := range strings.SplitSeq(string(rendered), "\n---") {
		jsonData, err := yaml.YAMLToJSON([]byte(doc))
		if err != nil {
			t.Fatalf("invalid YAML in ActorTemplate manifest: %v", err)
		}
		if string(jsonData) == "null" {
			continue
		}
		tmpl := &ateapipb.ActorTemplate{}
		if err := protojson.Unmarshal(jsonData, tmpl); err != nil {
			t.Fatalf("decoding protojson ActorTemplate: %v", err)
		}
		templates = append(templates, tmpl)
	}
	if len(templates) == 0 {
		t.Fatalf("ActorTemplate manifest holds no documents")
	}
	return templates
}

// SubstrateFixtureManifests names the two manifest templates a substrate
// fixture is built from (both repo-relative, under internal/e2e/fixtures).
type SubstrateFixtureManifests struct {
	// Pool declares the k8s side: a Namespace plus the WorkerPool CRD.
	Pool string
	// Template declares one or more protojson-shaped ActorTemplate
	// documents, separated by ---.
	Template string
}

// DeploySubstrateFixture installs a fixture for the sandbox class under test:
// it creates the fixture's atespace through the ate API, ko-applies the pool
// manifest, creates the fixture's ActorTemplates (ko-resolving the templates'
// ko:// image references first), and blocks until every template's golden
// snapshot exists. name distinguishes the caller (by convention its suite
// name): each suite gets its own copy of the fixture, so no suite's cleanup
// can delete it out from under another running concurrently.
//
// The run must create the atespace itself: that is what proves everything in
// it belongs to this run. If the atespace already exists (another run of the
// same suite is using it, or an interrupted run left it behind) the fixture
// fails before changing anything, in the atespace or in the cluster.
//
// Everything is removed when the test ends. The substrate resources need
// explicit cleanup — unlike the CRD templates they replaced, they do not ride
// the k8s namespace GC. A template is deleted after the actors in the
// fixture's atespace that still use it. If an actor in any other atespace
// still uses it, that actor, the template, the atespace and the pool with its
// namespace are all left in place, since the actor may be running on the
// pool's workers, and the test fails naming what was left.
//
// Returns the fixture's atespace (the same string that names the k8s
// namespace holding the pool) and the created templates.
func DeploySubstrateFixture(t *testing.T, ctx context.Context, clients *Clients, manifests SubstrateFixtureManifests, bucket, name string, trustBundle bool) (string, []*ateapipb.ActorTemplate) {
	t.Helper()

	tmplInline, tmplBlocks := substrateTemplateSubstitutions(bucket, name, trustBundle)
	rendered := koResolve(t, renderManifest(t, manifests.Template, tmplInline, tmplBlocks))
	templates := decodeSubstrateTemplates(t, rendered)

	atespace := templates[0].GetMetadata().GetAtespace()
	for _, tmpl := range templates {
		if got := tmpl.GetMetadata().GetAtespace(); got != atespace {
			t.Fatalf("fixture %s declares templates in different atespaces (%q and %q)", manifests.Template, atespace, got)
		}
	}

	// The pool manifest also carries the namespace. It is rendered before
	// the teardown is registered, so its file outlives the teardown that
	// deletes it (t.Cleanup is LIFO).
	inline, blocks := fixtureSubstitutions(bucket, name)
	poolManifest := renderManifest(t, manifests.Pool, inline, blocks)

	// Claimed before the pool is applied, so a run that cannot claim the
	// atespace also leaves the pool and its namespace alone.
	claim, err := fixturecleanup.ClaimAtespace(ctx, clients.SubstrateAPI, atespace)
	if err != nil {
		t.Fatalf("failed to claim the fixture's atespace: %v", err)
	}
	teardown := claim.NewTeardown()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := teardown.Run(cleanupCtx, t.Logf); err != nil {
			t.Errorf("fixture teardown: %v", err)
		}
	})

	// Set before the apply, so a partly applied pool is deleted too.
	teardown.SetPool(fmt.Sprintf("Namespace %q and its WorkerPool", atespace), func(ctx context.Context) error {
		return kubectlDelete(ctx, t, poolManifest)
	})
	koApply(t, poolManifest)

	created := make([]*ateapipb.ActorTemplate, 0, len(templates))
	for _, tmpl := range templates {
		c, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: tmpl})
		if err != nil {
			t.Fatalf("failed to create ActorTemplate %s/%s: %v", atespace, tmpl.GetMetadata().GetName(), err)
		}
		// Added before the golden wait so a template whose golden never
		// builds still gets cleaned up.
		teardown.AddTemplate(c.GetMetadata().GetName())
		created = append(created, c)
	}

	for _, tmpl := range created {
		t.Logf("Waiting for ActorTemplate %s/%s golden snapshot...", atespace, tmpl.GetMetadata().GetName())
		WaitForSubstrateTemplateReady(ctx, t, clients, atespace, tmpl.GetMetadata().GetName())
	}
	return atespace, created
}

// kubectlDelete deletes the objects in manifest, succeeding if they are
// already gone.
func kubectlDelete(ctx context.Context, t *testing.T, manifest string) error {
	args := []string{"delete", "--ignore-not-found", "-f", manifest}
	if KubeContext != "" {
		args = append([]string{"--context=" + KubeContext}, args...)
	}
	t.Logf("Running command: kubectl %s", strings.Join(args, " "))
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	t.Logf("%s", out)
	return nil
}
