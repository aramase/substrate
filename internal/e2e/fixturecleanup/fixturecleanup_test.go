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

package fixturecleanup

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The cmd/ateapi functional tests run a Claim against the real server; this
// fake covers the selection and error paths those do not reach.

const fixtureAtespace = "fixture"

var templateRef = &ateapipb.ObjectRef{Atespace: fixtureAtespace, Name: "tmpl"}

// fakeControl models the server's refusals: the template is in use while any
// actor names it or is listed in borrowers, CreateAtespace fails for an
// existing atespace, and DeleteAtespace fails for a non-empty one.
type fakeControl struct {
	ateapipb.ControlClient
	atespaces map[string]bool
	// createErr, if set, is what CreateAtespace returns.
	createErr error
	// templateExists says whether templateRef, the only template, exists.
	templateExists bool
	actors         []*ateapipb.Actor
	borrowers      int
	// templateErr, if set, is what DeleteActorTemplate returns.
	templateErr error
	pageSize    int

	listedAtespaces []string
	deleted         []*ateapipb.DeleteActorRequest
	// templateDeletes counts DeleteActorTemplate calls.
	templateDeletes int
}

func (f *fakeControl) CreateAtespace(_ context.Context, req *ateapipb.CreateAtespaceRequest, _ ...grpc.CallOption) (*ateapipb.Atespace, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	name := req.GetAtespace().GetMetadata().GetName()
	if f.atespaces[name] {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	if f.atespaces == nil {
		f.atespaces = map[string]bool{}
	}
	f.atespaces[name] = true
	return req.GetAtespace(), nil
}

func (f *fakeControl) DeleteAtespace(_ context.Context, req *ateapipb.DeleteAtespaceRequest, _ ...grpc.CallOption) (*ateapipb.Atespace, error) {
	name := req.GetAtespace().GetName()
	if !f.atespaces[name] {
		return nil, status.Error(codes.NotFound, "not found")
	}
	nonEmpty := f.templateExists && name == fixtureAtespace
	for _, a := range f.actors {
		if a.GetMetadata().GetAtespace() == name {
			nonEmpty = true
		}
	}
	if nonEmpty {
		return nil, status.Error(codes.FailedPrecondition, "not empty")
	}
	delete(f.atespaces, name)
	return &ateapipb.Atespace{}, nil
}

func actor(atespace, name string, template *ateapipb.ObjectRef) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name, Uid: "uid-" + atespace + "-" + name},
		ActorTemplate: template,
	}
}

func (f *fakeControl) DeleteActorTemplate(_ context.Context, req *ateapipb.DeleteActorTemplateRequest, _ ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	f.templateDeletes++
	if f.templateErr != nil {
		return nil, f.templateErr
	}
	// The fake holds one template, templateRef.
	if !f.templateExists || req.GetActorTemplate().GetName() != templateRef.GetName() {
		return nil, status.Error(codes.NotFound, "not found")
	}
	inUse := f.borrowers > 0
	for _, a := range f.actors {
		if a.GetActorTemplate().GetAtespace() == req.GetActorTemplate().GetAtespace() && a.GetActorTemplate().GetName() == req.GetActorTemplate().GetName() {
			inUse = true
		}
	}
	if inUse {
		return nil, status.Error(codes.FailedPrecondition, "in use")
	}
	f.templateExists = false
	return &ateapipb.ActorTemplate{}, nil
}

func (f *fakeControl) ListActors(_ context.Context, req *ateapipb.ListActorsRequest, _ ...grpc.CallOption) (*ateapipb.ListActorsResponse, error) {
	f.listedAtespaces = append(f.listedAtespaces, req.GetAtespace())
	var matching []*ateapipb.Actor
	for _, a := range f.actors {
		if req.GetAtespace() == "" || a.GetMetadata().GetAtespace() == req.GetAtespace() {
			matching = append(matching, a)
		}
	}
	start := 0
	if req.GetPageToken() != "" {
		start, _ = strconv.Atoi(req.GetPageToken())
	}
	end := min(start+f.pageSize, len(matching))
	resp := &ateapipb.ListActorsResponse{Actors: matching[start:end]}
	if end < len(matching) {
		resp.NextPageToken = strconv.Itoa(end)
	}
	return resp, nil
}

func (f *fakeControl) DeleteActor(_ context.Context, req *ateapipb.DeleteActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.deleted = append(f.deleted, req)
	f.actors = slices.DeleteFunc(f.actors, func(a *ateapipb.Actor) bool {
		return a.GetMetadata().GetAtespace() == req.GetActor().GetAtespace() && a.GetMetadata().GetName() == req.GetActor().GetName()
	})
	return &ateapipb.Actor{}, nil
}

func names(actors []*ateapipb.Actor) []string {
	var out []string
	for _, a := range actors {
		out = append(out, a.GetMetadata().GetAtespace()+"/"+a.GetMetadata().GetName())
	}
	return out
}

func TestClaimDeleteTemplate(t *testing.T) {
	otherTemplate := &ateapipb.ObjectRef{Atespace: fixtureAtespace, Name: "other"}
	sameNameElsewhere := &ateapipb.ObjectRef{Atespace: "team-b", Name: "tmpl"}
	tests := []struct {
		name           string
		fake           *fakeControl
		wantCode       codes.Code
		wantDeleted    []string
		wantRemaining  []string
		templateExists bool
	}{
		{
			name:     "template already gone",
			fake:     &fakeControl{actors: []*ateapipb.Actor{actor(fixtureAtespace, "a", otherTemplate)}},
			wantCode: codes.OK,
			// Nothing is listed or deleted when the template is absent.
			wantRemaining: []string{"fixture/a"},
		},
		{
			name: "only the fixture's own actors use it",
			fake: &fakeControl{templateExists: true, actors: []*ateapipb.Actor{
				actor(fixtureAtespace, "leaked-1", templateRef),
				actor(fixtureAtespace, "unrelated", otherTemplate),
				actor(fixtureAtespace, "leaked-2", templateRef),
				actor(fixtureAtespace, "same-name-elsewhere", sameNameElsewhere),
				actor(fixtureAtespace, "leaked-3", templateRef),
			}},
			wantCode:      codes.OK,
			wantDeleted:   []string{"fixture/leaked-1", "fixture/leaked-2", "fixture/leaked-3"},
			wantRemaining: []string{"fixture/unrelated", "fixture/same-name-elsewhere"},
		},
		{
			name: "an actor in another atespace uses it",
			fake: &fakeControl{templateExists: true, actors: []*ateapipb.Actor{
				actor(fixtureAtespace, "leaked", templateRef),
				actor("team-b", "foreign", templateRef),
			}},
			wantCode:       codes.FailedPrecondition,
			wantDeleted:    []string{"fixture/leaked"},
			wantRemaining:  []string{"team-b/foreign"},
			templateExists: true,
		},
		{
			name: "only a foreign actor uses it",
			fake: &fakeControl{templateExists: true, actors: []*ateapipb.Actor{
				actor("team-b", "foreign", templateRef),
			}},
			wantCode:       codes.FailedPrecondition,
			wantRemaining:  []string{"team-b/foreign"},
			templateExists: true,
		},
		{
			name: "an actor borrows the golden snapshot",
			fake: &fakeControl{templateExists: true, borrowers: 1, actors: []*ateapipb.Actor{
				actor(fixtureAtespace, "leaked", templateRef),
			}},
			wantCode:       codes.FailedPrecondition,
			wantDeleted:    []string{"fixture/leaked"},
			templateExists: true,
		},
		{
			name: "the delete fails for another reason",
			fake: &fakeControl{templateExists: true, templateErr: status.Error(codes.PermissionDenied, "denied"), actors: []*ateapipb.Actor{
				actor(fixtureAtespace, "leaked", templateRef),
			}},
			wantCode:       codes.PermissionDenied,
			wantRemaining:  []string{"fixture/leaked"},
			templateExists: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fake.pageSize = 2
			claim := &Claim{client: tt.fake, atespace: fixtureAtespace}
			err := claim.DeleteTemplate(context.Background(), templateRef.GetName(), t.Logf)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("DeleteTemplate() = %v, want code %v", err, tt.wantCode)
			}
			for _, a := range tt.fake.listedAtespaces {
				if a != fixtureAtespace {
					t.Errorf("listed actors in atespace %q, want only %q", a, fixtureAtespace)
				}
			}
			var deleted []string
			for _, req := range tt.fake.deleted {
				ref := req.GetActor()
				deleted = append(deleted, ref.GetAtespace()+"/"+ref.GetName())
				if !req.GetAnyState() {
					t.Errorf("DeleteActor(%s/%s) without any_state", ref.GetAtespace(), ref.GetName())
				}
				if want := "uid-" + ref.GetAtespace() + "-" + ref.GetName(); req.GetOptions().GetUid() != want {
					t.Errorf("DeleteActor(%s/%s) uid precondition = %q, want %q", ref.GetAtespace(), ref.GetName(), req.GetOptions().GetUid(), want)
				}
			}
			if diff := cmp.Diff(tt.wantDeleted, deleted); diff != "" {
				t.Errorf("deleted actors (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantRemaining, names(tt.fake.actors)); diff != "" {
				t.Errorf("remaining actors (-want +got):\n%s", diff)
			}
			if tt.fake.templateExists != tt.templateExists {
				t.Errorf("template exists = %v, want %v", tt.fake.templateExists, tt.templateExists)
			}
		})
	}
}

func TestClaimAtespace(t *testing.T) {
	t.Run("a new atespace is claimed and released", func(t *testing.T) {
		fake := &fakeControl{}
		claim, err := ClaimAtespace(context.Background(), fake, fixtureAtespace)
		if err != nil {
			t.Fatalf("ClaimAtespace() = %v", err)
		}
		if got := claim.Atespace(); got != fixtureAtespace {
			t.Errorf("Atespace() = %q, want %q", got, fixtureAtespace)
		}
		if err := claim.Release(context.Background()); err != nil {
			t.Fatalf("Release() = %v", err)
		}
		if fake.atespaces[fixtureAtespace] {
			t.Errorf("atespace %q still exists after Release", fixtureAtespace)
		}
		// A second Release finds nothing to delete.
		if err := claim.Release(context.Background()); err != nil {
			t.Errorf("Release() of a deleted atespace = %v, want nil", err)
		}
	})

	t.Run("an existing atespace is refused and left alone", func(t *testing.T) {
		// Another run's live actor and template are in the atespace.
		fake := &fakeControl{
			atespaces:      map[string]bool{fixtureAtespace: true},
			templateExists: true,
			actors:         []*ateapipb.Actor{actor(fixtureAtespace, "other-run", templateRef)},
		}
		claim, err := ClaimAtespace(context.Background(), fake, fixtureAtespace)
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("ClaimAtespace() = %v, want code AlreadyExists", err)
		}
		if claim != nil {
			t.Errorf("ClaimAtespace() returned a claim with its error")
		}
		if len(fake.listedAtespaces) != 0 || len(fake.deleted) != 0 || fake.templateDeletes != 0 {
			t.Errorf("refused claim touched the atespace: listed %v, deleted %d actors, %d template deletes", fake.listedAtespaces, len(fake.deleted), fake.templateDeletes)
		}
		if diff := cmp.Diff([]string{"fixture/other-run"}, names(fake.actors)); diff != "" {
			t.Errorf("remaining actors (-want +got):\n%s", diff)
		}
		if !fake.templateExists || !fake.atespaces[fixtureAtespace] {
			t.Errorf("template exists = %v, atespace exists = %v, want both", fake.templateExists, fake.atespaces[fixtureAtespace])
		}
	})

	t.Run("a failed create is returned", func(t *testing.T) {
		fake := &fakeControl{createErr: status.Error(codes.PermissionDenied, "denied")}
		if _, err := ClaimAtespace(context.Background(), fake, fixtureAtespace); status.Code(err) != codes.PermissionDenied {
			t.Errorf("ClaimAtespace() = %v, want code PermissionDenied", err)
		}
	})

	t.Run("a non-empty atespace is not released", func(t *testing.T) {
		fake := &fakeControl{}
		claim, err := ClaimAtespace(context.Background(), fake, fixtureAtespace)
		if err != nil {
			t.Fatalf("ClaimAtespace() = %v", err)
		}
		fake.actors = []*ateapipb.Actor{actor(fixtureAtespace, "a", templateRef)}
		if err := claim.Release(context.Background()); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("Release() of a non-empty atespace = %v, want code FailedPrecondition", err)
		}
	})
}

func TestTeardown(t *testing.T) {
	const poolName = `Namespace "fixture" and its WorkerPool`
	tests := []struct {
		name string
		fake *fakeControl
		// noPool leaves the pool unset, as when setup failed before it.
		noPool  bool
		poolErr error
		// wantCode is the code of Run's error; OK means it returns nil.
		wantCode codes.Code
		// wantInErr are substrings Run's error must contain.
		wantInErr      []string
		wantPoolDelete bool
		wantRemaining  []string
		wantTemplate   bool
		wantAtespace   bool
	}{
		{
			name: "only this run's actors use the templates",
			fake: &fakeControl{templateExists: true, actors: []*ateapipb.Actor{
				actor(fixtureAtespace, "own-1", templateRef),
				actor(fixtureAtespace, "own-2", templateRef),
			}},
			wantCode:       codes.OK,
			wantPoolDelete: true,
		},
		{
			name: "a foreign live actor uses a template",
			fake: &fakeControl{templateExists: true, actors: []*ateapipb.Actor{
				actor(fixtureAtespace, "own", templateRef),
				actor("team-b", "foreign", templateRef),
			}},
			wantCode:      codes.FailedPrecondition,
			wantInErr:     []string{"tmpl", `atespace "fixture"`, poolName},
			wantRemaining: []string{"team-b/foreign"},
			wantTemplate:  true,
			wantAtespace:  true,
		},
		{
			name: "an actor borrows a template's golden snapshot",
			fake: &fakeControl{templateExists: true, borrowers: 1},
			// The borrower's location is unknown, so it may be on the pool.
			wantCode:     codes.FailedPrecondition,
			wantInErr:    []string{"tmpl", poolName},
			wantTemplate: true,
			wantAtespace: true,
		},
		{
			name: "a template delete fails for another reason",
			fake: &fakeControl{templateExists: true, templateErr: status.Error(codes.Unavailable, "unavailable"), actors: []*ateapipb.Actor{
				actor("team-b", "foreign", templateRef),
			}},
			wantCode:      codes.Unavailable,
			wantInErr:     []string{"tmpl", poolName},
			wantRemaining: []string{"team-b/foreign"},
			wantTemplate:  true,
			wantAtespace:  true,
		},
		{
			name:           "the pool delete fails",
			fake:           &fakeControl{templateExists: true},
			poolErr:        status.Error(codes.Internal, "kubectl failed"),
			wantCode:       codes.Internal,
			wantInErr:      []string{poolName},
			wantPoolDelete: true,
			// The atespace is empty, so it is released anyway.
		},
		{
			name:     "setup failed before the pool was set",
			fake:     &fakeControl{templateExists: true},
			noPool:   true,
			wantCode: codes.OK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fake.pageSize = 2
			ctx := context.Background()
			claim, err := ClaimAtespace(ctx, tt.fake, fixtureAtespace)
			if err != nil {
				t.Fatalf("ClaimAtespace() = %v", err)
			}
			td := claim.NewTeardown()
			td.AddTemplate(templateRef.GetName())
			// A second template that is already gone counts as deleted.
			td.AddTemplate("gone")
			poolDeleted := false
			if !tt.noPool {
				td.SetPool(poolName, func(context.Context) error {
					// The pool goes only after every template.
					if tt.fake.templateExists {
						t.Errorf("pool deleted while template %s still exists", templateRef.GetName())
					}
					if !tt.fake.atespaces[fixtureAtespace] {
						t.Errorf("pool deleted after the atespace was released")
					}
					poolDeleted = true
					return tt.poolErr
				})
			}

			err = td.Run(ctx, t.Logf)
			if tt.wantCode == codes.OK {
				if err != nil {
					t.Fatalf("Run() = %v, want nil", err)
				}
			} else if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("Run() = %v, want code %v", err, tt.wantCode)
			}
			t.Logf("Run() = %v", err)
			for _, want := range tt.wantInErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Run() error does not name %q: %v", want, err)
				}
			}
			if poolDeleted != tt.wantPoolDelete {
				t.Errorf("pool deleted = %v, want %v", poolDeleted, tt.wantPoolDelete)
			}
			if diff := cmp.Diff(tt.wantRemaining, names(tt.fake.actors)); diff != "" {
				t.Errorf("remaining actors (-want +got):\n%s", diff)
			}
			if tt.fake.templateExists != tt.wantTemplate {
				t.Errorf("template exists = %v, want %v", tt.fake.templateExists, tt.wantTemplate)
			}
			if got := tt.fake.atespaces[fixtureAtespace]; got != tt.wantAtespace {
				t.Errorf("atespace exists = %v, want %v", got, tt.wantAtespace)
			}
		})
	}

	t.Run("a rerun finishes once the foreign actor is gone", func(t *testing.T) {
		ctx := context.Background()
		fake := &fakeControl{pageSize: 2, templateExists: true}
		claim, err := ClaimAtespace(ctx, fake, fixtureAtespace)
		if err != nil {
			t.Fatalf("ClaimAtespace() = %v", err)
		}
		fake.actors = []*ateapipb.Actor{actor("team-b", "foreign", templateRef)}
		td := claim.NewTeardown()
		td.AddTemplate(templateRef.GetName())
		poolDeletes := 0
		td.SetPool(poolName, func(context.Context) error {
			poolDeletes++
			return nil
		})
		if err := td.Run(ctx, t.Logf); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("Run() with a foreign actor = %v, want code FailedPrecondition", err)
		}
		if poolDeletes != 0 {
			t.Fatalf("pool deleted %d times while a foreign actor uses the template", poolDeletes)
		}
		fake.actors = nil
		if err := td.Run(ctx, t.Logf); err != nil {
			t.Fatalf("Run() after the foreign actor is gone = %v", err)
		}
		if poolDeletes != 1 || fake.templateExists || fake.atespaces[fixtureAtespace] {
			t.Errorf("after rerun: pool deletes = %d, template exists = %v, atespace exists = %v; want 1, false, false", poolDeletes, fake.templateExists, fake.atespaces[fixtureAtespace])
		}
	})
}
