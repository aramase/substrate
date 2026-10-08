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

// Package fixturecleanup gives an e2e run exclusive ownership of a fixture's
// atespace and removes what the run created there, touching nothing it cannot
// prove it owns.
package fixturecleanup

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Claim is an atespace this run created. The server refuses to delete a
// non-empty atespace, so a newly created one holds nothing, and everything
// in it was created after the claim, by this run.
type Claim struct {
	client   ateapipb.ControlClient
	atespace string
}

// ClaimAtespace creates the atespace for this run's exclusive use.
//
// If the atespace already exists, another run is using it or an earlier run
// leaked it, and this run cannot tell which. ClaimAtespace then deletes
// nothing and returns an error wrapping the server's AlreadyExists.
func ClaimAtespace(ctx context.Context, client ateapipb.ControlClient, atespace string) (*Claim, error) {
	_, err := client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: atespace}}})
	if status.Code(err) == codes.AlreadyExists {
		return nil, fmt.Errorf("atespace %q already exists: another run of this fixture is using it, or an earlier run left it behind, and this run cannot tell which, so it changes nothing in it. If no other run uses it, delete its actors, its ActorTemplates and the atespace, then rerun: %w", atespace, err)
	}
	if err != nil {
		return nil, fmt.Errorf("creating atespace %q: %w", atespace, err)
	}
	return &Claim{client: client, atespace: atespace}, nil
}

// Atespace returns the claimed atespace's name.
func (c *Claim) Atespace() string {
	return c.atespace
}

// DeleteTemplate deletes the claimed atespace's ActorTemplate name. The
// server refuses that while any actor uses the template, so the actors in
// the claimed atespace that name it are deleted first, in any state, each
// pinned to the uid it was listed with. Actors in other atespaces are never
// listed or deleted. If the delete is still refused, an actor this run does
// not own (one in another atespace, or one that borrows the template's golden
// snapshot) uses the template, and DeleteTemplate returns the refusal.
// A template that does not exist counts as deleted.
func (c *Claim) DeleteTemplate(ctx context.Context, name string, logf func(format string, args ...any)) error {
	ref := &ateapipb.ObjectRef{Atespace: c.atespace, Name: name}
	refusal := c.deleteTemplate(ctx, ref)
	if status.Code(refusal) != codes.FailedPrecondition {
		return refusal
	}
	deleted, err := c.deleteActorsUsing(ctx, ref, logf)
	if err != nil {
		return err
	}
	if deleted > 0 {
		refusal = c.deleteTemplate(ctx, ref)
		if status.Code(refusal) != codes.FailedPrecondition {
			return refusal
		}
	}
	return fmt.Errorf("ActorTemplate %s/%s is still in use after deleting this run's %d actors that use it; the remaining users are in other atespaces or borrow its golden snapshot, and this run does not own them, so it leaves them and the template for an operator: %w",
		ref.GetAtespace(), ref.GetName(), deleted, refusal)
}

// Release deletes the claimed atespace. The server refuses that while
// anything is left in it.
func (c *Claim) Release(ctx context.Context) error {
	_, err := c.client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: c.atespace}})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("deleting atespace %q: %w", c.atespace, err)
	}
	return nil
}

func (c *Claim) deleteTemplate(ctx context.Context, ref *ateapipb.ObjectRef) error {
	_, err := c.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: ref})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

// deleteActorsUsing deletes, in any state, the actors in the claimed atespace
// whose actor_template is ref, and returns how many it deleted.
func (c *Claim) deleteActorsUsing(ctx context.Context, ref *ateapipb.ObjectRef, logf func(format string, args ...any)) (int, error) {
	var owned []*ateapipb.Actor
	pageToken := ""
	for {
		resp, err := c.client.ListActors(ctx, &ateapipb.ListActorsRequest{Atespace: c.atespace, PageSize: 1000, PageToken: pageToken})
		if err != nil {
			return 0, fmt.Errorf("listing actors in atespace %q: %w", c.atespace, err)
		}
		for _, actor := range resp.GetActors() {
			// The atespace check repeats the request's filter, so a server
			// that ignored it still cannot widen what is deleted.
			if actor.GetMetadata().GetAtespace() != c.atespace ||
				actor.GetActorTemplate().GetAtespace() != ref.GetAtespace() ||
				actor.GetActorTemplate().GetName() != ref.GetName() {
				continue
			}
			owned = append(owned, actor)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	// Listing finishes before any delete so pagination does not shift under it.
	for _, actor := range owned {
		actorRef := &ateapipb.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()}
		logf("Deleting actor %s/%s left by this run, which uses ActorTemplate %s/%s", actorRef.GetAtespace(), actorRef.GetName(), ref.GetAtespace(), ref.GetName())
		_, err := c.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor:    actorRef,
			AnyState: true,
			Options:  &ateapipb.DeleteOptions{Uid: actor.GetMetadata().GetUid()},
		})
		if err != nil && status.Code(err) != codes.NotFound {
			return 0, fmt.Errorf("deleting actor %s/%s: %w", actorRef.GetAtespace(), actorRef.GetName(), err)
		}
	}
	return len(owned), nil
}
