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

package functionaltest

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/internal/resources"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestActorTemplateCRUD(t *testing.T) {
	ns := namespaceForTest("ns-template-crud")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ensureDefaultGvisorSandboxConfig(t, tc)

	created, err := tc.client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata:       &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-a"},
			Containers:     []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
			SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: "gs://my-bucket/snapshots"},
			SandboxConfig: &ateapipb.SandboxConfig{
				SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
				ConfigName:   "gvisor-default",
			},
			Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "1Gi"}}},
			// Server-owned status on the request is ignored.
			Status: &ateapipb.ActorTemplateStatus{
				GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
					GoldenTag: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "golden-tag"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	want := &ateapipb.ActorTemplate{
		Metadata:   &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-a", Version: 1},
		Containers: []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation:   "gs://my-bucket/snapshots",
			PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   "gvisor-default",
		},
		Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "1Gi"}}},
		Status:    &ateapipb.ActorTemplateStatus{},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActorTemplate response mismatch (-want +got):\n%s", diff)
	}

	_, err = tc.client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata:       &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-a"},
			Containers:     []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
			SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: "gs://my-bucket/snapshots"},
			SandboxConfig:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
		},
	})
	assertGrpcError(t, err, codes.AlreadyExists, "ActorTemplate "+testAtespace+"/tmpl-a already exists")

	got, err := tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	if err != nil {
		t.Fatalf("GetActorTemplate failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetActorTemplate response mismatch (-created +got):\n%s", diff)
	}

	list, err := tc.client.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{})
	if err != nil {
		t.Fatalf("ListActorTemplates failed: %v", err)
	}
	if len(list.GetActorTemplates()) != 1 || list.GetActorTemplates()[0].GetMetadata().GetName() != "tmpl-a" {
		t.Errorf("ListActorTemplates = %v, want [tmpl-a]", list.GetActorTemplates())
	}

	// The atespace filter scopes the listing: a match returns the template, a
	// different atespace returns nothing.
	list, err = tc.client.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListActorTemplates(atespace) failed: %v", err)
	}
	if len(list.GetActorTemplates()) != 1 {
		t.Errorf("ListActorTemplates(atespace=%s) = %v, want [tmpl-a]", testAtespace, list.GetActorTemplates())
	}
	list, err = tc.client.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{Atespace: "other-atespace"})
	if err != nil {
		t.Fatalf("ListActorTemplates(other atespace) failed: %v", err)
	}
	if len(list.GetActorTemplates()) != 0 {
		t.Errorf("ListActorTemplates(atespace=other-atespace) = %v, want []", list.GetActorTemplates())
	}

	deleted, err := tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	if err != nil {
		t.Fatalf("DeleteActorTemplate failed: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform()); diff != "" {
		t.Errorf("DeleteActorTemplate response mismatch (-created +deleted):\n%s", diff)
	}
	_, err = tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	assertGrpcError(t, err, codes.NotFound, "ActorTemplate "+testAtespace+"/tmpl-a not found")
	_, err = tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	assertGrpcError(t, err, codes.NotFound, "ActorTemplate "+testAtespace+"/tmpl-a not found")

	// config_name is required: a template must name its SandboxConfig.
	_, err = tc.client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata:       &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-unnamed-config"},
			Containers:     []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
			SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: "gs://my-bucket/snapshots"},
			SandboxConfig:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
		},
	})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, `sandbox_config\.config_name`)
}

func TestGoldenTagLifecycle(t *testing.T) {
	ns := namespaceForTest("golden-tag")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	tmpl := createTemplateWithSelector(t, tc, "golden-template", &ateapipb.Selector{MatchLabels: map[string]string{poolLabelKey: ns}})
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl)
	// Created before readiness: a later golden tag must not change its source.
	early, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "early"}, ActorTemplate: templateRef.ToObjectRef(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	controlapi.NewActorTemplateReconciler(tc.persistence, tc.service, 7*time.Second).Start(ctx)
	var goldenRef *ateapipb.ObjectRef
	err = wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		current, err := tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: templateRef.ToObjectRef()})
		goldenRef = current.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()
		return goldenRef != nil, err
	})
	if err != nil {
		t.Fatalf("waiting for golden tag: %v", err)
	}
	golden, err := tc.client.GetTag(ctx, &ateapipb.GetTagRequest{Tag: goldenRef})
	if err != nil {
		t.Fatal(err)
	}
	uri := golden.GetStatus().GetSnapshot().GetSnapshotUri()
	parsed, err := resources.ParseSnapshotURI(uri)
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.OwnedBy(resources.TagSnapshotOwner(resources.GoldenActorAtespace, parsed.Name())) {
		t.Fatal("golden snapshot is not tag-owned")
	}
	assertSnapshotPresent(t, tc, uri)
	_, err = tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: goldenRef})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("golden actor was not deleted: %v", err)
	}
	late, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "late"}, ActorTemplate: templateRef.ToObjectRef(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if late.GetStatus().GetExternalSnapshot().GetSnapshotUri() != uri || late.GetStatus().GetExternalSnapshot().GetActorTemplateUid() != tmpl.GetMetadata().GetUid() {
		t.Fatal("actor did not inherit golden tag snapshot and template UID")
	}
	waitForWorkerAvailable(t, tc, workerName)
	tc.fakeAtelet.Lock.Lock()
	tc.fakeAtelet.RunCalled = false
	tc.fakeAtelet.Lock.Unlock()
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: resources.ActorRefFromActor(early).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	if !tc.fakeAtelet.RunCalled {
		t.Fatal("actor created before golden readiness did not cold boot")
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: resources.ActorRefFromActor(early).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: resources.ActorRefFromActor(late).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	if !tc.fakeAtelet.RestoreCalled {
		t.Fatal("actor did not restore golden tag")
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: resources.ActorRefFromActor(late).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	assertSnapshotPresent(t, tc, uri)
	for _, actor := range []*ateapipb.Actor{early, late} {
		if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: resources.ActorRefFromActor(actor).ToObjectRef(), AnyState: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: templateRef.ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	assertSnapshotCollected(t, tc, uri)
	_, err = tc.client.GetTag(ctx, &ateapipb.GetTagRequest{Tag: goldenRef})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("golden tag was not deleted: %v", err)
	}
}

func TestListActorTemplates_InvalidPageToken(t *testing.T) {
	ns := namespaceForTest("ns-template-invalid-token")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.ListActorTemplates(context.Background(),
		&ateapipb.ListActorTemplatesRequest{PageToken: "%%%"})
	assertGrpcError(t, err, codes.InvalidArgument, "invalid page_token")
}

func TestDeleteActorTemplate_Preconditions(t *testing.T) {
	ns := namespaceForTest("ns-delete-template-preconditions")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	tmpl := createTemplateWithSelector(t, tc, "tmpl1", nil)
	templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl)
	ref := templateRef.ToObjectRef()
	del := func(opts *ateapipb.DeleteOptions) error {
		_, err := tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: ref, Options: opts})
		return err
	}

	uid, version := tmpl.GetMetadata().GetUid(), tmpl.GetMetadata().GetVersion()

	assertGrpcError(t, del(&ateapipb.DeleteOptions{Version: version + 1}), codes.Aborted, "concurrent update conflict, please retry")
	assertGrpcError(t, del(&ateapipb.DeleteOptions{Uid: uid, Version: version + 1}), codes.Aborted, "concurrent update conflict, please retry")
	assertGrpcError(t, del(&ateapipb.DeleteOptions{Uid: foreignUID}), codes.Aborted, "ActorTemplate "+templateRef.String()+" does not have uid "+foreignUID)
	assertGrpcError(t, del(&ateapipb.DeleteOptions{Uid: foreignUID, Version: version}), codes.Aborted, "ActorTemplate "+templateRef.String()+" does not have uid "+foreignUID)
	if _, err := tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: ref}); err != nil {
		t.Fatalf("a refused delete removed the template: %v", err)
	}

	if err := del(&ateapipb.DeleteOptions{Version: version}); err != nil {
		t.Fatalf("DeleteActorTemplate with the matching version: %v", err)
	}
	tmpl = createTemplateWithSelector(t, tc, "tmpl1", nil)
	if err := del(&ateapipb.DeleteOptions{Uid: tmpl.GetMetadata().GetUid()}); err != nil {
		t.Fatalf("DeleteActorTemplate with the matching uid: %v", err)
	}
	tmpl = createTemplateWithSelector(t, tc, "tmpl1", nil)
	if err := del(&ateapipb.DeleteOptions{Uid: tmpl.GetMetadata().GetUid(), Version: tmpl.GetMetadata().GetVersion()}); err != nil {
		t.Fatalf("DeleteActorTemplate with both guards: %v", err)
	}
	_, err := tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: ref})
	assertGrpcError(t, err, codes.NotFound, "ActorTemplate "+templateRef.String()+" not found")
}

// buildGoldenTemplate creates a template and runs the reconciler until its
// golden tag is ready. It returns the template as stored then, and the golden
// tag.
func buildGoldenTemplate(t *testing.T, ctx context.Context, tc *testContext, ns string) (*ateapipb.ActorTemplate, *ateapipb.Tag) {
	t.Helper()
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	created := createTemplateWithSelector(t, tc, "golden-template", &ateapipb.Selector{MatchLabels: map[string]string{poolLabelKey: ns}})
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	templateRef := resources.ActorTemplateRefFromActorTemplate(created).ToObjectRef()
	controlapi.NewActorTemplateReconciler(tc.persistence, tc.service, 7*time.Second).Start(ctx)
	var tmpl *ateapipb.ActorTemplate
	err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		var err error
		tmpl, err = tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: templateRef})
		return tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil, err
	})
	if err != nil {
		t.Fatalf("waiting for golden tag: %v", err)
	}
	golden, err := tc.client.GetTag(ctx, &ateapipb.GetTagRequest{Tag: tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()})
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotPresent(t, tc, golden.GetStatus().GetSnapshot().GetSnapshotUri())
	return tmpl, golden
}

// TestDeleteActorTemplate_RefusedWhileActorsExist checks that a template with
// actors in any atespace is not deleted, that the refusal leaves the template
// and its golden tag and snapshot alone, and that the delete collects them
// once the actors are gone.
func TestDeleteActorTemplate_RefusedWhileActorsExist(t *testing.T) {
	ns := namespaceForTest("tmpl-in-use")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tmpl, golden := buildGoldenTemplate(t, ctx, tc, ns)
	templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()
	goldenURI := golden.GetStatus().GetSnapshot().GetSnapshotUri()

	const otherAtespace = "team-b"
	if _, err := tc.client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: otherAtespace}}}); err != nil {
		t.Fatal(err)
	}
	users := []resources.ActorRef{{Atespace: testAtespace, Name: "local"}, {Atespace: otherAtespace, Name: "remote"}}
	for _, ref := range users {
		if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name}, ActorTemplate: templateRef,
		}}); err != nil {
			t.Fatalf("CreateActor(%s): %v", ref, err)
		}
	}

	_, err := tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: templateRef})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteActorTemplate with actors = %v, want code FailedPrecondition", err)
	}
	// The template's own atespace is named; the other atespace's actor is
	// only counted, since deleting the template grants no read on it.
	msg := status.Convert(err).Message()
	if want := "in use by actors " + users[0].String() + ", and by 1 actor in another atespace;"; !strings.Contains(msg, want) {
		t.Errorf("DeleteActorTemplate error %q does not contain %q", msg, want)
	}
	if strings.Contains(msg, users[1].Name) {
		t.Errorf("DeleteActorTemplate error %q names %s, an actor outside the template's atespace", msg, users[1])
	}
	after, err := tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: templateRef})
	if err != nil {
		t.Fatalf("GetActorTemplate after the refused delete: %v", err)
	}
	if diff := cmp.Diff(tmpl, after, protocmp.Transform()); diff != "" {
		t.Errorf("template changed by the refused delete (-before +after):\n%s", diff)
	}
	if _, err := tc.client.GetTag(ctx, &ateapipb.GetTagRequest{Tag: resources.TagRefFromTag(golden).ToObjectRef()}); err != nil {
		t.Errorf("golden tag after the refused delete: %v", err)
	}
	assertSnapshotPresent(t, tc, goldenURI)

	for _, ref := range users {
		if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref.ToObjectRef()}); err != nil {
			t.Fatalf("DeleteActor(%s): %v", ref, err)
		}
	}
	if _, err := tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: templateRef}); err != nil {
		t.Fatalf("DeleteActorTemplate once its actors are gone: %v", err)
	}
	if _, err := tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: templateRef}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActorTemplate after delete = %v, want NotFound", err)
	}
	if _, err := tc.client.GetTag(ctx, &ateapipb.GetTagRequest{Tag: resources.TagRefFromTag(golden).ToObjectRef()}); status.Code(err) != codes.NotFound {
		t.Errorf("golden tag after delete = %v, want NotFound", err)
	}
	assertSnapshotCollected(t, tc, goldenURI)
}

// TestDeleteActorTemplate_ConcurrentCreateActor creates an actor from the
// template while the delete is collecting the golden snapshot. Either the
// create fails, or it lands first and the delete is refused; it must never
// leave an actor naming a deleted template.
func TestDeleteActorTemplate_ConcurrentCreateActor(t *testing.T) {
	ns := namespaceForTest("tmpl-delete-race")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tmpl, golden := buildGoldenTemplate(t, ctx, tc, ns)
	templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()
	actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "racer"}

	createErr := make(chan error, 1)
	// The hook runs on the server's goroutine.
	var started atomic.Bool
	tc.objectStore.OnDelete = func(_, key string) error {
		if !strings.Contains(key, "/tags/") || started.Swap(true) {
			return nil
		}
		go func() {
			_, err := tc.client.CreateActor(context.WithoutCancel(ctx), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.GetAtespace(), Name: actorRef.GetName()}, ActorTemplate: templateRef,
			}})
			createErr <- err
		}()
		// Give the create time to finish if nothing holds it back.
		select {
		case err := <-createErr:
			createErr <- err
		case <-time.After(2 * time.Second):
		}
		return nil
	}

	_, deleteErr := tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: templateRef})
	if !started.Load() {
		t.Fatalf("the delete never collected the golden snapshot: %v", deleteErr)
	}
	var err error
	select {
	case err = <-createErr:
	case <-time.After(30 * time.Second):
		t.Fatal("CreateActor did not return after the delete")
	}
	if deleteErr != nil {
		t.Fatalf("DeleteActorTemplate: %v", deleteErr)
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor during the delete = %v, want code FailedPrecondition", err)
	}
	if _, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after the delete = %v, want NotFound", err)
	}
	if _, err := tc.client.GetTag(ctx, &ateapipb.GetTagRequest{Tag: resources.TagRefFromTag(golden).ToObjectRef()}); status.Code(err) != codes.NotFound {
		t.Errorf("golden tag after delete = %v, want NotFound", err)
	}
}

// TestDeleteActorTemplate_RepointedActorBorrowingGolden follows the recovery
// the refusal names for an actor that was never resumed: it still borrows the
// golden snapshot after a repoint, and a suspend alone does not release it,
// since SuspendActor on a suspended actor changes nothing. Resuming it on the
// new template and suspending it once moves it to a snapshot of its own, and
// then the delete goes through and the actor still resumes.
func TestDeleteActorTemplate_RepointedActorBorrowingGolden(t *testing.T) {
	ns := namespaceForTest("tmpl-repoint")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tmpl, golden := buildGoldenTemplate(t, ctx, tc, ns)
	templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()
	goldenURI := golden.GetStatus().GetSnapshot().GetSnapshotUri()

	// waitForIdleWorker waits for the pool's one worker to host nothing, so a
	// resume has somewhere to go.
	waitForIdleWorker := func() {
		t.Helper()
		err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 30*time.Second, true, func(context.Context) (bool, error) {
			workers, err := tc.workerCache.Workers()
			if err != nil || len(workers) != 1 {
				return false, nil
			}
			w := workers[0]
			return w.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_ACTIVE && w.GetStatus().GetAllocated().GetActors() == 0, nil
		})
		if err != nil {
			t.Fatalf("waiting for an idle worker: %v", err)
		}
	}

	// The replacement runs on the same pool. Its own golden build uses the
	// worker, so wait for it before resuming anything there.
	replacement := createTemplateWithSelector(t, tc, "replacement", &ateapipb.Selector{MatchLabels: map[string]string{poolLabelKey: ns}})
	replacementRef := resources.ActorTemplateRefFromActorTemplate(replacement).ToObjectRef()
	err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		got, err := tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: replacementRef})
		return got.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil, err
	})
	if err != nil {
		t.Fatalf("waiting for the replacement's golden tag: %v", err)
	}
	waitForIdleWorker()

	actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"}
	created, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.GetAtespace(), Name: actorRef.GetName()}, ActorTemplate: templateRef,
	}})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	if got := created.GetStatus().GetExternalSnapshot().GetSnapshotUri(); got != goldenURI {
		t.Fatalf("new actor's external snapshot = %q, want the golden %q", got, goldenURI)
	}
	if _, err := tc.client.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata: created.GetMetadata(), ActorTemplate: replacementRef,
	}}); err != nil {
		t.Fatalf("UpdateActor repointing at the replacement: %v", err)
	}

	assertRefused := func(when string) {
		t.Helper()
		_, err := tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: templateRef})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("DeleteActorTemplate %s = %v, want code FailedPrecondition", when, err)
		}
		if want := "resume and suspend them once"; !strings.Contains(status.Convert(err).Message(), want) {
			t.Errorf("DeleteActorTemplate %s error %q does not contain %q", when, status.Convert(err).Message(), want)
		}
		assertSnapshotPresent(t, tc, goldenURI)
	}
	assertRefused("after the repoint")
	suspended, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("SuspendActor of the suspended actor: %v", err)
	}
	if got := suspended.GetActor().GetStatus().GetExternalSnapshot().GetSnapshotUri(); got != goldenURI {
		t.Fatalf("external snapshot after a suspend without a resume = %q, want the golden %q still", got, goldenURI)
	}
	assertRefused("after a suspend without a resume")

	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("ResumeActor on the replacement: %v", err)
	}
	suspended, err = tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("SuspendActor after the resume: %v", err)
	}
	ownURI := suspended.GetActor().GetStatus().GetExternalSnapshot().GetSnapshotUri()
	assertSnapshotOwnedByActor(t, suspended.GetActor(), ownURI)

	if _, err := tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: templateRef}); err != nil {
		t.Fatalf("DeleteActorTemplate once the actor has a snapshot of its own: %v", err)
	}
	assertSnapshotCollected(t, tc, goldenURI)
	assertSnapshotPresent(t, tc, ownURI)
	waitForIdleWorker()
	resumed, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("ResumeActor after the old template's delete: %v", err)
	}
	if got := resumed.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state after the resume = %v, want RUNNING", got)
	}
}
