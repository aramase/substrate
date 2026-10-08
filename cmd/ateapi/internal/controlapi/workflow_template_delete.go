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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// DeleteActorTemplate executes the workflow to delete an ActorTemplate. It
// refuses while any actor still uses the template (see
// actorBlocksTemplateDelete). Otherwise it marks the template deleting, which
// from then on refuses creating an actor from it or repointing one at it,
// removes the golden actor and tag, and then removes the row. Each step is its
// own store write, so the cleanup holds no transaction while it calls atelet
// and object storage. The row goes last, so a failed attempt leaves it marked
// for a retry to rediscover the golden resources by.
func (w *ActorWorkflow) DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions) (*ateapipb.ActorTemplate, error) {
	// Serializes against the reconciler, which creates the golden actor and
	// tag under the same lease.
	ctx, lease, err := acquireLease(ctx, w.store, "lease:actortemplate:"+templateRef.Atespace+":"+templateRef.Name, "ActorTemplate "+templateRef.String())
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	tmpl, err := w.loadTemplateForDelete(ctx, templateRef)
	if err != nil {
		return nil, err
	}
	// Both are named after the template's uid, in the reserved atespace. The
	// lease keeps the uid and the golden tag stable until the delete ends.
	uid := tmpl.GetMetadata().GetUid()
	goldenRef := resources.ActorRef{Atespace: resources.GoldenActorAtespace, Name: uid}
	goldenTagRef := resources.TagRef{Atespace: resources.GoldenActorAtespace, Name: uid}
	goldenURI, err := w.loadGoldenSnapshotURI(ctx, goldenTagRef)
	if err != nil {
		return nil, err
	}
	if precondition.UID == "" {
		// Pin every later step to the template the golden refs were derived
		// from.
		precondition.UID = uid
	}

	// A stale caller or a refused delete must not destroy the golden
	// resources, so the cleanup runs only once the template is marked.
	if err := w.markTemplateDeleting(ctx, templateRef, precondition, func(_ *ateapipb.ActorTemplate, actor *ateapipb.Actor) bool {
		return actorBlocksTemplateDelete(actor, templateRef, goldenRef, goldenURI)
	}); err != nil {
		return nil, err
	}
	if err := w.ensureGoldenActorDeleted(ctx, goldenRef); err != nil {
		return nil, err
	}
	if err := w.ensureGoldenTagDeleted(ctx, goldenTagRef); err != nil {
		return nil, err
	}
	return w.finalizeTemplateDeleted(ctx, templateRef, store.DeletePreconditions{UID: uid})
}

// actorBlocksTemplateDelete reports whether actor keeps the template from
// being deleted: it names the template, or it still borrows the golden tag's
// snapshot, which the delete would collect. An actor repointed elsewhere
// borrows it until it is resumed and then suspended; SuspendActor on an actor
// that is already suspended takes no snapshot. The template's own golden
// actor does not count; the delete removes it.
func actorBlocksTemplateDelete(actor *ateapipb.Actor, templateRef resources.ActorTemplateRef, goldenRef resources.ActorRef, goldenURI string) bool {
	if resources.ActorRefFromActor(actor) == goldenRef {
		return false
	}
	if resources.ActorTemplateRefFromObjectRef(actor.GetActorTemplate()) == templateRef {
		return true
	}
	return goldenURI != "" && actor.GetStatus().GetExternalSnapshot().GetSnapshotUri() == goldenURI
}

// loadTemplateForDelete fetches the current template record.
func (w *ActorWorkflow) loadTemplateForDelete(ctx context.Context, templateRef resources.ActorTemplateRef) (_ *ateapipb.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadTemplateForDelete")
	defer func() { err = done(err) }()

	tmpl, err := w.store.GetActorTemplate(ctx, templateRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("ActorTemplate %s not found", templateRef)
		}
		return nil, fmt.Errorf("while getting actor template %s: %w", templateRef, err)
	}
	return tmpl, nil
}

// loadGoldenSnapshotURI returns the URI of the snapshot the golden tag holds,
// or "" if there is no tag or it never finished.
func (w *ActorWorkflow) loadGoldenSnapshotURI(ctx context.Context, goldenTagRef resources.TagRef) (_ string, err error) {
	ctx, done := stepSpan(ctx, "LoadGoldenSnapshotURI")
	defer func() { err = done(err) }()

	tag, err := w.store.GetTag(ctx, goldenTagRef)
	if errors.Is(err, store.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("while getting golden tag: %w", err)
	}
	return tag.GetStatus().GetSnapshot().GetSnapshotUri(), nil
}

// ensureGoldenActorDeleted removes the template's golden actor, whatever state
// it is in. Absent is the ordinary case for a template whose golden snapshot
// never finished.
func (w *ActorWorkflow) ensureGoldenActorDeleted(ctx context.Context, goldenRef resources.ActorRef) (err error) {
	ctx, done := stepSpan(ctx, "DeleteGoldenActor")
	defer func() { err = done(err) }()

	if _, err := w.DeleteActor(ctx, goldenRef, true, store.DeletePreconditions{}); err != nil {
		if apierror.Code(err) == codes.NotFound {
			markSkipped(ctx, "golden actor already deleted")
			return nil
		}
		return fmt.Errorf("while deleting golden actor: %w", err)
	}
	return nil
}

// ensureGoldenTagDeleted removes the template's golden tag and the snapshot it
// holds.
func (w *ActorWorkflow) ensureGoldenTagDeleted(ctx context.Context, goldenTagRef resources.TagRef) (err error) {
	ctx, done := stepSpan(ctx, "DeleteGoldenTag")
	defer func() { err = done(err) }()

	if _, err := w.DeleteTag(ctx, goldenTagRef, store.DeletePreconditions{}); err != nil {
		if apierror.Code(err) == codes.NotFound {
			markSkipped(ctx, "golden tag already deleted")
			return nil
		}
		return fmt.Errorf("while deleting golden tag: %w", err)
	}
	return nil
}

// markTemplateDeleting checks precondition and blocks with the template
// locked and marks it deleting. Neither a refusal nor a precondition failure
// changes anything.
func (w *ActorWorkflow) markTemplateDeleting(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions, blocks func(*ateapipb.ActorTemplate, *ateapipb.Actor) bool) (err error) {
	ctx, done := stepSpan(ctx, "MarkTemplateDeleting")
	defer func() { err = done(err) }()

	if _, err := w.store.MarkActorTemplateDeleting(ctx, templateRef, precondition, blocks); err != nil {
		if inUse := (*store.ActorTemplateInUseError)(nil); errors.As(err, &inUse) {
			return templateInUseError(templateRef, inUse)
		}
		return templateDeleteStoreError(templateRef, precondition, err, "while marking actor template deleting")
	}
	return nil
}

// finalizeTemplateDeleted removes the marked template from the store and
// returns the deleted record.
func (w *ActorWorkflow) finalizeTemplateDeleted(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions) (_ *ateapipb.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTemplateDeleted")
	defer func() { err = done(err) }()

	deleted, err := w.store.DeleteActorTemplate(ctx, templateRef, precondition)
	if err != nil {
		return nil, templateDeleteStoreError(templateRef, precondition, err, "while deleting actor template from DB")
	}
	return deleted, nil
}

// templateDeleteStoreError maps a store error from either delete step.
func templateDeleteStoreError(templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions, err error, while string) error {
	if errors.Is(err, store.ErrNotFound) {
		return apierror.NotFound("ActorTemplate %s not found", templateRef)
	}
	if errors.Is(err, store.ErrUIDConflict) {
		return apierror.Aborted("ActorTemplate %s does not have uid %s", templateRef, precondition.UID)
	}
	if errors.Is(err, store.ErrVersionConflict) {
		return apierror.Aborted("concurrent update conflict, please retry")
	}
	return fmt.Errorf("%s: %w", while, err)
}

// templateInUseError describes the actors that keep templateRef from being
// deleted. It names actors only in the template's atespace, where the
// caller's can_delete on the template implies can_get on actors, and counts
// the ones in other atespaces.
func templateInUseError(templateRef resources.ActorTemplateRef, inUse *store.ActorTemplateInUseError) error {
	var users []string
	if len(inUse.Actors) > 0 {
		names := make([]string, 0, len(inUse.Actors))
		for _, ref := range inUse.Actors {
			names = append(names, ref.String())
		}
		named := "actors " + strings.Join(names, ", ")
		if inUse.More {
			named += " and others"
		}
		users = append(users, named)
	}
	switch n := inUse.OtherAtespaces; {
	case n == 1:
		users = append(users, "1 actor in another atespace")
	case n > 1:
		users = append(users, fmt.Sprintf("%d actors in other atespaces", n))
	}
	return apierror.FailedPrecondition("ActorTemplate %s is in use by %s; delete them, or repoint them to another template, then resume and suspend them once, and retry", templateRef, strings.Join(users, ", and by "))
}
