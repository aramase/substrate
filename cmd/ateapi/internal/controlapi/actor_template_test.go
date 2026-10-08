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

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// validActorTemplate returns the smallest template that passes create
// validation; mutations tweak it per test case. The snapshot scopes and
// resume policy are set explicitly because TestValidateActorTemplate
// exercises validation directly, without defaulting.
func validActorTemplate(mutations ...func(*ateapipb.ActorTemplate)) *ateapipb.ActorTemplate {
	template := &ateapipb.ActorTemplate{
		Metadata:   &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tmpl-a"},
		Containers: []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation:   "gs://my-bucket/snapshots",
			PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
	}
	for _, m := range mutations {
		m(template)
	}
	return template
}

// gvisorDefaultLister returns a SandboxConfig lister seeded with the
// "gvisor-default" config that validActorTemplate names.
func gvisorDefaultLister(t *testing.T) listersv1alpha1.SandboxConfigLister {
	t.Helper()
	return sandboxConfigListerFor(t, []*atev1alpha1.SandboxConfig{{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-default"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass:   atev1alpha1.SandboxClassGvisor,
			DefaultVersion: "v1",
			Versions: []atev1alpha1.SandboxVersionConfig{{
				Name:       "v1",
				PauseImage: "registry.k8s.io/pause@sha256:x",
				Assets:     testAssets(),
			}},
		},
	}})
}

// TestCreateActorTemplate_SandboxConfigChecks pins the create-time checks on
// the template's named SandboxConfig: it must exist and match the template's
// class, both FailedPrecondition — they depend on cluster state, and the
// lister may briefly lag a just-created config, so the error is retryable.
func TestCreateActorTemplate_SandboxConfigChecks(t *testing.T) {
	persistence := newTestPersistence(t)
	s := &RPCService{impl: newServiceImpl(persistence, nil), sandboxConfigLister: gvisorDefaultLister(t)}
	ctx := context.Background()
	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}

	tests := []struct {
		name     string
		sandbox  *ateapipb.SandboxConfig
		wantCode codes.Code
	}{{
		name:     "named config exists and matches",
		sandbox:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
		wantCode: codes.OK,
	}, {
		name:     "empty config_name is rejected",
		sandbox:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM},
		wantCode: codes.InvalidArgument,
	}, {
		name:     "named config missing",
		sandbox:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "does-not-exist"},
		wantCode: codes.FailedPrecondition,
	}, {
		name:     "named config class mismatch",
		sandbox:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, ConfigName: "gvisor-default"},
		wantCode: codes.FailedPrecondition,
	}}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
				tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: "ns1", Name: fmt.Sprintf("tmpl-%d", i)}
				tmpl.SandboxConfig = tt.sandbox
			})}
			_, err := s.CreateActorTemplate(ctx, req)
			if apierror.Code(err) != tt.wantCode {
				t.Errorf("CreateActorTemplate error = %v, want code %v", err, tt.wantCode)
			}
		})
	}
}

// TestCreateActorTemplate covers the atespace precondition: creation fails
// while the atespace is missing, and succeeds once the atespace exists.
func TestCreateActorTemplate(t *testing.T) {
	persistence := newTestPersistence(t)
	s := &RPCService{impl: newServiceImpl(persistence, nil), sandboxConfigLister: gvisorDefaultLister(t)}
	ctx := context.Background()
	req := func(atespace, name string) *ateapipb.CreateActorTemplateRequest {
		return &ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}
		})}
	}

	if _, err := s.CreateActorTemplate(ctx, req("ns-missing", "tmpl-a")); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActorTemplate in missing atespace = %v, want FailedPrecondition", err)
	}

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	created, err := s.CreateActorTemplate(ctx, req("ns1", "tmpl-a"))
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	if created.GetMetadata().GetName() != "tmpl-a" {
		t.Errorf("created name = %q, want tmpl-a", created.GetMetadata().GetName())
	}
}

// TestCreateActorTemplateIgnoresServerOwnedFields pins the create contract:
// status on the request is dropped and new templates start with an empty
// status. The store persists whatever the handler hands it, so the handler is
// the only guard.
func TestCreateActorTemplateIgnoresServerOwnedFields(t *testing.T) {
	persistence := newTestPersistence(t)
	s := &RPCService{impl: newServiceImpl(persistence, nil), sandboxConfigLister: gvisorDefaultLister(t)}
	ctx := context.Background()

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}

	in := validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
		tmpl.Metadata.Uid = "11111111-1111-1111-1111-111111111111"
		tmpl.Metadata.Version = 42
		tmpl.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"pool": "default"}}
		tmpl.Containers = []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}}
		tmpl.Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "1Gi"}}}
		// Server-owned status a client must not be able to set.
		tmpl.Status = &ateapipb.ActorTemplateStatus{
			GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
				GoldenTag: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "golden-tag"},
			},
		}
	})
	created, err := s.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: in})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}

	want := validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
		tmpl.Metadata.Version = 1
		tmpl.WorkerSelector = in.GetWorkerSelector()
		tmpl.Containers = in.GetContainers()
		tmpl.Resources = in.GetResources()
		tmpl.Status = &ateapipb.ActorTemplateStatus{}
	})
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActorTemplate response mismatch (-want +got):\n%s", diff)
	}
	if got := created.GetMetadata().GetUid(); got == "" || got == in.GetMetadata().GetUid() {
		t.Errorf("created uid = %q, want a fresh server-assigned uid", got)
	}
}

func TestDeleteActorTemplate(t *testing.T) {
	tests := []struct {
		name         string
		actorDeleted bool
		tagDeleted   bool
		pendingTag   bool
		// failPrefix makes object storage fail cleanup for this resource kind.
		failPrefix            string
		wantActorAfterFailure bool
		staleGuard            bool
	}{
		{name: "golden actor and tag"},
		{name: "golden actor already deleted", actorDeleted: true},
		{name: "golden tag absent", tagDeleted: true},
		{name: "no golden resources", actorDeleted: true, tagDeleted: true},
		{name: "incomplete golden tag", pendingTag: true},
		{name: "actor cleanup failure", failPrefix: "/actors/", wantActorAfterFailure: true},
		{name: "tag cleanup failure", failPrefix: "/tags/"},
		{name: "stale guard refused before cleanup", staleGuard: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			tmpl := seedSubstrateTemplate(t, ctx, persistence, "tmpl")
			templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl)
			goldenRef := resources.ActorRef{Atespace: resources.GoldenActorAtespace, Name: tmpl.GetMetadata().GetUid()}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: goldenRef.Atespace, Name: goldenRef.Name},
				ActorTemplate: templateRef.ToObjectRef(),
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			})
			workflow, objects := newFinalizeWorkflow(persistence)
			actorURI := mustActorSnapshotURI(t, tmpl, actor, "snapshot")
			objects.PutSnapshot(t, actorURI, "manifest.json")
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.ExternalSnapshot = &ateapipb.ExternalSnapshot{
					SnapshotUri:      actorURI.String(),
					Fidelity:         ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
					ActorTemplateUid: tmpl.GetMetadata().GetUid(),
				}
			})
			var tag *ateapipb.Tag
			if tt.pendingTag {
				tag = storetest.MustCreateTag(t, ctx, persistence, newPendingTestTag(t, goldenRef.Name, actor))
			} else {
				var err error
				tag, err = workflow.TagActorSnapshot(ctx, tagToCreate(goldenRef, goldenRef.Name))
				if err != nil {
					t.Fatal(err)
				}
			}
			tagRef := resources.TagRefFromTag(tag)
			tagURI := mustReservedTagSnapshotURI(t, tag)
			objects.PutSnapshot(t, tagURI, "manifest.json")
			svc := &RPCService{impl: newServiceImpl(persistence, nil), actorWorkflow: workflow, snapshotPlugin: objectstoreplugintest.ControlClient(objects)}
			// The handler must request AnyState to clean up an active golden actor.
			mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
			})
			if tt.actorDeleted {
				if _, err := workflow.DeleteActor(ctx, goldenRef, true, store.DeletePreconditions{}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.tagDeleted {
				if _, err := svc.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: tagRef.ToObjectRef()}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.failPrefix != "" {
				objects.OnDelete = func(_, key string) error {
					if strings.Contains(key, tt.failPrefix) {
						return errObjectStore
					}
					return nil
				}
			}
			if tt.staleGuard {
				current, err := persistence.GetActorTemplate(ctx, templateRef)
				if err != nil {
					t.Fatal(err)
				}
				stale := store.DeletePreconditions{UID: current.GetMetadata().GetUid(), Version: current.GetMetadata().GetVersion() + 1}
				if _, err := workflow.DeleteActorTemplate(ctx, templateRef, stale); apierror.Code(err) != codes.Aborted {
					t.Fatalf("DeleteActorTemplate with a stale version = %v, want code Aborted", err)
				}
				if _, err := persistence.GetActor(ctx, goldenRef); err != nil {
					t.Fatalf("golden actor after the refused delete: %v", err)
				}
				if _, err := persistence.GetTag(ctx, tagRef); err != nil {
					t.Fatalf("golden tag after the refused delete: %v", err)
				}
			}
			req := &ateapipb.DeleteActorTemplateRequest{ActorTemplate: templateRef.ToObjectRef()}
			deleted, err := svc.DeleteActorTemplate(ctx, req)
			if tt.failPrefix != "" {
				if !isObjectStoreErr(err) {
					t.Fatalf("DeleteActorTemplate = %v, want object storage error", err)
				}
				if _, err := persistence.GetActorTemplate(ctx, templateRef); err != nil {
					t.Fatalf("template lost after cleanup failure: %v", err)
				}
				if _, err := persistence.GetTag(ctx, tagRef); err != nil {
					t.Fatalf("tag lost after cleanup failure: %v", err)
				}
				_, actorErr := persistence.GetActor(ctx, goldenRef)
				if tt.wantActorAfterFailure && actorErr != nil || !tt.wantActorAfterFailure && !errors.Is(actorErr, store.ErrNotFound) {
					t.Fatalf("GetActor after failure = %v, want present %v", actorErr, tt.wantActorAfterFailure)
				}
				objects.OnDelete = nil
				deleted, err = svc.DeleteActorTemplate(ctx, req)
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tmpl, deleted, protocmp.Transform()); diff != "" {
				t.Fatalf("deleted template mismatch (-want +got):\n%s", diff)
			}
			if _, err := persistence.GetActorTemplate(ctx, templateRef); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("GetActorTemplate after delete = %v, want NotFound", err)
			}
			if _, err := persistence.GetActor(ctx, goldenRef); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("GetActor after delete = %v, want NotFound", err)
			}
			if _, err := persistence.GetTag(ctx, tagRef); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("GetTag after delete = %v, want NotFound", err)
			}
			for _, uri := range []resources.SnapshotURI{actorURI, tagURI} {
				if got := objects.Snapshot(t, uri); len(got) != 0 {
					t.Errorf("snapshot %s still holds %v", uri, got)
				}
			}
			if _, err := svc.DeleteActorTemplate(ctx, req); apierror.Code(err) != codes.NotFound {
				t.Fatalf("delete missing template = %v, want NotFound", err)
			}
		})
	}
}

// seedSubstrateTemplate stores a minimal substrate ActorTemplate in team-a.
func seedSubstrateTemplate(t *testing.T, ctx context.Context, persistence store.Interface, name string) *ateapipb.ActorTemplate {
	t.Helper()
	created, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: name},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation: "gs://ate-snapshots/team-a/",
		},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   "gvisor",
		},
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	stored, err := persistence.GetActorTemplate(ctx, resources.ActorTemplateRefFromActorTemplate(created))
	if err != nil {
		t.Fatalf("GetActorTemplate: %v", err)
	}
	return stored
}

// TestResolveActorTemplate verifies the resolver reads the substrate resource
// the actor's actor_template reference names.
func TestResolveActorTemplate(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	stored := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")

	t.Run("ref reads the store", func(t *testing.T) {
		actor := &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"}}
		got, err := resolveActorTemplate(ctx, persistence, actor)
		if err != nil {
			t.Fatalf("resolveActorTemplate: %v", err)
		}
		if got.GetMetadata().GetUid() != stored.GetMetadata().GetUid() {
			t.Errorf("template uid = %q, want the stored substrate template %q", got.GetMetadata().GetUid(), stored.GetMetadata().GetUid())
		}
	})

	t.Run("ref to a missing template is FailedPrecondition", func(t *testing.T) {
		actor := &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "absent"}}
		_, err := resolveActorTemplate(ctx, persistence, actor)
		if got := apierror.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("status.Code = %v, want FailedPrecondition (err: %v)", got, err)
		}
	})
}

// TestResolveActorTemplate_NotFound verifies a vanished template and an actor
// naming no template at all surface errActorTemplateNotFound, so callers like
// delete can tolerate them.
func TestResolveActorTemplate_NotFound(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	stored := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")

	tests := []struct {
		name         string
		actor        *ateapipb.Actor
		wantNotFound bool
	}{
		{"ref resolves", &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"}}, false},
		{"ref to deleted template", &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "gone"}}, true},
		{"no template named at all", &ateapipb.Actor{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveActorTemplate(ctx, persistence, tc.actor)
			if tc.wantNotFound {
				if !errors.Is(err, errActorTemplateNotFound) {
					t.Fatalf("resolveActorTemplate err = %v, want errActorTemplateNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveActorTemplate: %v", err)
			}
			if got.GetMetadata().GetUid() != stored.GetMetadata().GetUid() {
				t.Errorf("template uid = %q, want %q", got.GetMetadata().GetUid(), stored.GetMetadata().GetUid())
			}
		})
	}
}

// TestUpdateActorTemplateMetadata pins the store's update behavior: the
// server-assigned metadata is recomputed in place, and metadata identity is
// immutable.
func TestUpdateActorTemplateMetadata(t *testing.T) {
	persistence := newTestPersistence(t)
	ctx := context.Background()

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	created, err := persistence.CreateActorTemplate(ctx, validActorTemplate())
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	ref := resources.ActorTemplateRefFromActorTemplate(created)

	// A server-owned status write passes validation and bumps the version.
	updated, err := persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(created), func(tmpl *ateapipb.ActorTemplate) error {
		tmpl.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
			GoldenTag: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "golden-tag"},
		}}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActorTemplate failed: %v", err)
	}
	if got, want := updated.GetMetadata().GetVersion(), created.GetMetadata().GetVersion()+1; got != want {
		t.Errorf("updated version = %d, want %d", got, want)
	}

	// A mutation that touches an immutable field is a server bug, rejected by
	// the store.
	for name, mutate := range map[string]func(*ateapipb.ActorTemplate) error{
		"atespace": func(tmpl *ateapipb.ActorTemplate) error { tmpl.Metadata.Atespace = "ns2"; return nil },
		"name":     func(tmpl *ateapipb.ActorTemplate) error { tmpl.Metadata.Name = "tmpl-b"; return nil },
	} {
		if _, err := persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(updated), mutate); err == nil {
			t.Errorf("mutating %s succeeded, want error", name)
		}
	}

	// Server-assigned metadata edits are overwritten, not errors: the store
	// restores them from the stored value.
	reverted, err := persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(updated), func(tmpl *ateapipb.ActorTemplate) error {
		tmpl.Metadata.Uid = "1e186271-b829-4085-b2b1-6b665c1a4f42"
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActorTemplate with uid edit failed: %v", err)
	}
	if got, want := reverted.GetMetadata().GetUid(), created.GetMetadata().GetUid(); got != want {
		t.Errorf("uid after update = %q, want %q", got, want)
	}
}

// goldenTemplateFixture is a template whose golden snapshot finished: the
// golden tag holds a snapshot in object storage and the template's status
// names it. The golden actor is still present, as when a golden build is cut
// short.
type goldenTemplateFixture struct {
	tmpl        *ateapipb.ActorTemplate
	templateRef resources.ActorTemplateRef
	goldenRef   resources.ActorRef
	tagRef      resources.TagRef
	tagURI      resources.SnapshotURI
	workflow    *ActorWorkflow
	objects     *objectstoretest.Fake
	svc         *RPCService
}

func seedGoldenTemplate(t *testing.T, ctx context.Context, persistence store.Interface) goldenTemplateFixture {
	t.Helper()
	return seedNamedGoldenTemplate(t, ctx, persistence, "tmpl")
}

func seedNamedGoldenTemplate(t *testing.T, ctx context.Context, persistence store.Interface, name string) goldenTemplateFixture {
	t.Helper()
	tmpl := seedSubstrateTemplate(t, ctx, persistence, name)
	templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl)
	goldenRef := resources.ActorRef{Atespace: resources.GoldenActorAtespace, Name: tmpl.GetMetadata().GetUid()}
	golden := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: goldenRef.Atespace, Name: goldenRef.Name},
		ActorTemplate: templateRef.ToObjectRef(),
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	workflow, objects := newFinalizeWorkflow(persistence)
	actorURI := mustActorSnapshotURI(t, tmpl, golden, "snapshot")
	objects.PutSnapshot(t, actorURI, "manifest.json")
	mustUpdateActorStatus(t, ctx, persistence, golden, func(s *ateapipb.ActorStatus) {
		s.ExternalSnapshot = &ateapipb.ExternalSnapshot{
			SnapshotUri:      actorURI.String(),
			Fidelity:         ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			ActorTemplateUid: tmpl.GetMetadata().GetUid(),
		}
	})
	toCreate := tagToCreate(goldenRef, goldenRef.Name)
	toCreate.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
	tag, err := workflow.TagActorSnapshot(ctx, toCreate)
	if err != nil {
		t.Fatal(err)
	}
	tagRef := resources.TagRefFromTag(tag)
	tagURI := mustReservedTagSnapshotURI(t, tag)
	objects.PutSnapshot(t, tagURI, "manifest.json")
	tmpl, err = persistence.UpdateActorTemplate(ctx, templateRef, store.PreconditionFrom(tmpl), func(tmpl *ateapipb.ActorTemplate) error {
		tmpl.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{GoldenTag: tagRef.ToObjectRef()}}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return goldenTemplateFixture{
		tmpl:        tmpl,
		templateRef: templateRef,
		goldenRef:   goldenRef,
		tagRef:      tagRef,
		tagURI:      tagURI,
		workflow:    workflow,
		objects:     objects,
		svc:         &RPCService{impl: newServiceImpl(persistence, nil), actorWorkflow: workflow, snapshotPlugin: objectstoreplugintest.ControlClient(objects)},
	}
}

// assertIntact checks that a refused delete changed nothing.
func (f goldenTemplateFixture) assertIntact(t *testing.T, ctx context.Context, persistence store.Interface) {
	t.Helper()
	got, err := persistence.GetActorTemplate(ctx, f.templateRef)
	if err != nil {
		t.Fatalf("template after the refused delete: %v", err)
	}
	if diff := cmp.Diff(f.tmpl, got, protocmp.Transform()); diff != "" {
		t.Errorf("template changed by the refused delete (-before +after):\n%s", diff)
	}
	if _, err := persistence.GetActor(ctx, f.goldenRef); err != nil {
		t.Errorf("golden actor after the refused delete: %v", err)
	}
	if _, err := persistence.GetTag(ctx, f.tagRef); err != nil {
		t.Errorf("golden tag after the refused delete: %v", err)
	}
	if got := f.objects.Snapshot(t, f.tagURI); len(got) == 0 {
		t.Error("golden snapshot collected by the refused delete")
	}
}

// assertDeleted checks that a delete removed the template and collected its
// golden actor, tag and snapshot.
func (f goldenTemplateFixture) assertDeleted(t *testing.T, ctx context.Context, persistence store.Interface) {
	t.Helper()
	if _, err := persistence.GetActorTemplate(ctx, f.templateRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActorTemplate after delete = %v, want NotFound", err)
	}
	if _, err := persistence.GetActor(ctx, f.goldenRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActor(golden) after delete = %v, want NotFound", err)
	}
	if _, err := persistence.GetTag(ctx, f.tagRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetTag(golden) after delete = %v, want NotFound", err)
	}
	if got := f.objects.Snapshot(t, f.tagURI); len(got) != 0 {
		t.Errorf("golden snapshot still holds %v", got)
	}
}

// TestDeleteActorTemplate_RefusedWhileInUse checks that a template is not
// deleted while an actor still names it or borrows its golden snapshot, that
// the refusal changes nothing, and that the delete goes through once those
// actors are gone.
func TestDeleteActorTemplate_RefusedWhileInUse(t *testing.T) {
	tests := []struct {
		name string
		// seed stores the actor that keeps the template in use.
		seed func(t *testing.T, ctx context.Context, persistence store.Interface, f goldenTemplateFixture) resources.ActorRef
		// wantInError is what the refusal says about the actor. Actors in
		// other atespaces are counted, never named.
		wantInError func(user resources.ActorRef) string
	}{{
		name: "actor in the template's atespace",
		seed: func(t *testing.T, ctx context.Context, persistence store.Interface, f goldenTemplateFixture) resources.ActorRef {
			ref := resources.ActorRef{Atespace: f.templateRef.Atespace, Name: "user"}
			seedWorkflowActor(t, ctx, persistence, ref, f.templateRef.Atespace, f.templateRef.Name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
			return ref
		},
	}, {
		name: "actor in another atespace",
		seed: func(t *testing.T, ctx context.Context, persistence store.Interface, f goldenTemplateFixture) resources.ActorRef {
			ref := resources.ActorRef{Atespace: "team-b", Name: "user"}
			seedWorkflowActor(t, ctx, persistence, ref, f.templateRef.Atespace, f.templateRef.Name, ateapipb.ActorState_ACTOR_STATE_RUNNING)
			return ref
		},
		wantInError: func(resources.ActorRef) string { return "in use by 1 actor in another atespace;" },
	}, {
		name: "actor repointed elsewhere that still borrows the golden snapshot",
		seed: func(t *testing.T, ctx context.Context, persistence store.Interface, f goldenTemplateFixture) resources.ActorRef {
			ref := resources.ActorRef{Atespace: f.templateRef.Atespace, Name: "repointed"}
			seedWorkflowActor(t, ctx, persistence, ref, f.templateRef.Atespace, "tmpl-v2", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, func(a *ateapipb.Actor) {
				a.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{
					SnapshotUri:      f.tagURI.String(),
					Fidelity:         ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
					ActorTemplateUid: f.tmpl.GetMetadata().GetUid(),
				}
			})
			return ref
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			f := seedGoldenTemplate(t, ctx, persistence)
			user := tt.seed(t, ctx, persistence, f)
			req := &ateapipb.DeleteActorTemplateRequest{ActorTemplate: f.templateRef.ToObjectRef()}

			_, err := f.svc.DeleteActorTemplate(ctx, req)
			if apierror.Code(err) != codes.FailedPrecondition {
				t.Fatalf("DeleteActorTemplate while in use = %v, want code FailedPrecondition", err)
			}
			want := "in use by actors " + user.String() + ";"
			if tt.wantInError != nil {
				want = tt.wantInError(user)
				if strings.Contains(err.Error(), user.Name) {
					t.Errorf("DeleteActorTemplate error %q names %s, an actor outside the template's atespace", err, user)
				}
			}
			if !strings.Contains(err.Error(), want) {
				t.Errorf("DeleteActorTemplate error %q does not contain %q", err, want)
			}
			f.assertIntact(t, ctx, persistence)

			if _, err := f.workflow.DeleteActor(ctx, user, true, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteActor(%s): %v", user, err)
			}
			if _, err := f.svc.DeleteActorTemplate(ctx, req); err != nil {
				t.Fatalf("DeleteActorTemplate once unused: %v", err)
			}
			f.assertDeleted(t, ctx, persistence)
		})
	}
}

// TestDeleteActorTemplate_InUseErrorNamesFewActors checks that the refusal
// names a bounded number of the actors using the template, all in the
// template's atespace, and only counts the ones in other atespaces.
func TestDeleteActorTemplate_InUseErrorNamesFewActors(t *testing.T) {
	ctx := t.Context()
	persistence := newTestPersistence(t)
	f := seedGoldenTemplate(t, ctx, persistence)
	const users, remoteUsers = 8, 3
	for i := range users {
		ref := resources.ActorRef{Atespace: f.templateRef.Atespace, Name: fmt.Sprintf("user-%d", i)}
		seedWorkflowActor(t, ctx, persistence, ref, f.templateRef.Atespace, f.templateRef.Name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}
	for i := range remoteUsers {
		ref := resources.ActorRef{Atespace: "team-b", Name: fmt.Sprintf("remote-%d", i)}
		seedWorkflowActor(t, ctx, persistence, ref, f.templateRef.Atespace, f.templateRef.Name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}
	_, err := f.svc.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: f.templateRef.ToObjectRef()})
	if apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteActorTemplate while in use = %v, want code FailedPrecondition", err)
	}
	named := strings.Count(err.Error(), f.templateRef.Atespace+"/user-")
	if named == 0 || named >= users {
		t.Errorf("DeleteActorTemplate error %q names %d of %d actors, want some but not all", err, named, users)
	}
	if !strings.Contains(err.Error(), "and others") {
		t.Errorf("DeleteActorTemplate error %q does not say more actors use the template", err)
	}
	if strings.Contains(err.Error(), "remote-") {
		t.Errorf("DeleteActorTemplate error %q names actors outside the template's atespace", err)
	}
	if want := fmt.Sprintf("and by %d actors in other atespaces;", remoteUsers); !strings.Contains(err.Error(), want) {
		t.Errorf("DeleteActorTemplate error %q does not contain %q", err, want)
	}
	f.assertIntact(t, ctx, persistence)
}

// signalingCreateStore reports when CreateActor reaches the store, which is
// after the service resolved the template and its golden tag.
type signalingCreateStore struct {
	store.Interface
	reached chan struct{}
}

func (s *signalingCreateStore) CreateActor(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	close(s.reached)
	return s.Interface.CreateActor(ctx, actor)
}

// TestDeleteActorTemplate_ConcurrentCreateActor creates an actor from the
// template while the delete is collecting the golden snapshot, after the
// create already resolved the template and borrowed that snapshot. The create
// must fail rather than leave an actor naming a deleted template and
// borrowing a deleted snapshot.
func TestDeleteActorTemplate_ConcurrentCreateActor(t *testing.T) {
	ctx := t.Context()
	persistence := newTestPersistence(t)
	f := seedGoldenTemplate(t, ctx, persistence)
	creating := &signalingCreateStore{Interface: persistence, reached: make(chan struct{})}
	creator := newServiceImpl(creating, nil)
	actorRef := resources.ActorRef{Atespace: f.templateRef.Atespace, Name: "racer"}

	type result struct {
		actor *ateapipb.Actor
		err   error
	}
	created := make(chan result, 1)
	var started bool
	f.objects.OnDelete = func(_, key string) error {
		if started || !strings.Contains(key, "/tags/") {
			return nil
		}
		started = true
		go func() {
			a, err := creator.CreateActor(context.WithoutCancel(ctx), &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: f.templateRef.ToObjectRef(),
			})
			created <- result{a, err}
		}()
		select {
		case <-creating.reached:
		case <-time.After(30 * time.Second):
			t.Error("CreateActor never reached the store")
			return nil
		}
		// Give the create time to finish if nothing holds it back.
		select {
		case r := <-created:
			created <- r
		case <-time.After(2 * time.Second):
		}
		return nil
	}

	if _, err := f.svc.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: f.templateRef.ToObjectRef()}); err != nil {
		t.Fatalf("DeleteActorTemplate: %v", err)
	}
	if !started {
		t.Fatal("the delete never collected the golden snapshot")
	}
	var r result
	select {
	case r = <-created:
	case <-time.After(30 * time.Second):
		t.Fatal("CreateActor did not return after the delete")
	}
	if r.err == nil {
		t.Errorf("CreateActor during the delete succeeded with snapshot %q, want an error", r.actor.GetStatus().GetExternalSnapshot().GetSnapshotUri())
	} else if apierror.Code(r.err) != codes.FailedPrecondition {
		t.Errorf("CreateActor during the delete = %v, want code FailedPrecondition", r.err)
	}
	if _, err := persistence.GetActor(ctx, actorRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActor(%s) after the delete = %v, want NotFound", actorRef, err)
	}
	f.assertDeleted(t, ctx, persistence)
}

// TestDeleteActorTemplate_ConcurrentDeletesFitThePool runs as many template
// deletes at once as the store's pool has connections. The golden cleanup
// takes leases and writes through the same pool, so a delete that held a
// connection across it would leave every delete waiting on a second one.
func TestDeleteActorTemplate_ConcurrentDeletesFitThePool(t *testing.T) {
	for _, n := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("%d deletes, %d connections", n, n), func(t *testing.T) {
			seeded := storetest.SetupPostgresPersistence(t)
			ctx := t.Context()
			storetest.MustCreateAtespace(t, ctx, seeded, "team-a")
			fixtures := make([]goldenTemplateFixture, n)
			for i := range fixtures {
				fixtures[i] = seedNamedGoldenTemplate(t, ctx, seeded, fmt.Sprintf("tmpl-%d", i))
			}
			persistence := storetest.ReopenWithMaxConns(t, seeded, int32(n))

			deleteCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i, f := range fixtures {
				workflow := &ActorWorkflow{store: persistence, snapshotPlugin: objectstoreplugintest.ControlClient(f.objects)}
				wg.Go(func() {
					_, errs[i] = workflow.DeleteActorTemplate(deleteCtx, f.templateRef, store.DeletePreconditions{})
				})
			}
			wg.Wait()
			for i, f := range fixtures {
				if errs[i] != nil {
					t.Errorf("DeleteActorTemplate(%s): %v", f.templateRef, errs[i])
					continue
				}
				f.assertDeleted(t, ctx, persistence)
			}
		})
	}
}

// TestDeleteActorTemplate_RetryAfterFailedCleanup fails the golden snapshot
// collection once. The template must stay, refusing new actors, until a retry
// finishes the cleanup.
func TestDeleteActorTemplate_RetryAfterFailedCleanup(t *testing.T) {
	ctx := t.Context()
	persistence := newTestPersistence(t)
	f := seedGoldenTemplate(t, ctx, persistence)
	errStorage := errors.New("object storage unavailable")
	failing := true
	f.objects.OnDelete = func(_, key string) error {
		if failing && strings.Contains(key, "/tags/") {
			return errStorage
		}
		return nil
	}
	req := &ateapipb.DeleteActorTemplateRequest{ActorTemplate: f.templateRef.ToObjectRef()}

	if _, err := f.svc.DeleteActorTemplate(ctx, req); err == nil {
		t.Fatal("DeleteActorTemplate with failing object storage succeeded, want an error")
	}
	if _, err := persistence.GetActorTemplate(ctx, f.templateRef); err != nil {
		t.Fatalf("template after the failed delete: %v", err)
	}
	_, err := newServiceImpl(persistence, nil).CreateActor(ctx, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: f.templateRef.Atespace, Name: "late"},
		ActorTemplate: f.templateRef.ToObjectRef(),
	})
	if apierror.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "is being deleted") {
		t.Errorf("CreateActor from a template whose delete failed part-way = %v, want FailedPrecondition saying it is being deleted", err)
	}

	failing = false
	if _, err := f.svc.DeleteActorTemplate(ctx, req); err != nil {
		t.Fatalf("DeleteActorTemplate retry: %v", err)
	}
	f.assertDeleted(t, ctx, persistence)
}
