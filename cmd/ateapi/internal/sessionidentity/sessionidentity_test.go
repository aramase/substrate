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

package sessionidentity

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/k8sjwt"
)

func TestSafeClientJWTClaimsLogsIdentityInClear(t *testing.T) {
	claims := &k8sjwt.KubernetesClaims{
		Issuer:             "issuer",
		Subject:            "subject",
		Audiences:          []string{"audience"},
		Expiration:         time.Unix(100, 0),
		NotBefore:          time.Unix(90, 0),
		IssuedAt:           time.Unix(80, 0),
		JTI:                "jti-value",
		Namespace:          "namespace-value",
		ServiceAccountName: "service-account-value",
		ServiceAccountUID:  "service-account-uid-value",
		PodName:            "pod-value",
		PodUID:             "pod-uid-value",
		SecretName:         "secret-name-value",
		SecretUID:          "secret-uid-value",
		NodeName:           "node-value",
		NodeUID:            "node-uid-value",
		WarnAfter:          time.Unix(95, 0),
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.InfoContext(context.Background(), "Verified client JWT", slog.Attr{
		Key:   "claims",
		Value: safeClientJWTClaims(claims),
	})

	line := buf.String()
	// Every claim is non-secret K8s identity/validity metadata and is logged in
	// clear for the auth audit trail.
	for _, want := range []string{
		"issuer", "subject", "audience",
		"jti-value", "namespace-value", "service-account-value",
		"service-account-uid-value", "pod-value", "pod-uid-value",
		"secret-name-value", "secret-uid-value", "node-value", "node-uid-value",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("log output missing identity claim %q: %s", want, line)
		}
	}
	if strings.Contains(line, "REDACTED") {
		t.Errorf("no identity claim should be redacted: %s", line)
	}
}
