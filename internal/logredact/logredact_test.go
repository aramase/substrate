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

package logredact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestHandlerRedactsCredentialFormats(t *testing.T) {
	const (
		jwt           = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJzIn0.signature"
		bearer        = "Bearer bearer-token.abc123"
		authorization = "authorization=token-value"
		privateKey    = "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----"
	)

	var buf bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil))).With(
		slog.String("with_bearer", bearer),
	)
	logger.InfoContext(context.Background(),
		"verified "+jwt,
		slog.String("authorization", authorization),
		slog.String("private_key", privateKey),
		slog.Group("nested",
			slog.String("normal", "ordinary value"),
			slog.String("jwt", jwt),
		),
	)

	line := buf.String()
	for _, secret := range []string{jwt, "bearer-token.abc123", "token-value", privateKey, "secret"} {
		if strings.Contains(line, secret) {
			t.Errorf("log output contains secret %q: %s", secret, line)
		}
	}
	for _, want := range []string{"ordinary value", redacted} {
		if !strings.Contains(line, want) {
			t.Errorf("log output does not contain %q: %s", want, line)
		}
	}
}

func TestHandlerLeavesNormalStringsAndNestedGroupsIntact(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))
	logger.InfoContext(context.Background(),
		"normal message",
		slog.String("plain", "hello"),
		slog.Group("nested", slog.String("plain", "world")),
	)

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}
	if got["msg"] != "normal message" {
		t.Errorf("msg = %v, want normal message", got["msg"])
	}
	if got["plain"] != "hello" {
		t.Errorf("plain = %v, want hello", got["plain"])
	}
	nested, ok := got["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested = %T, want object", got["nested"])
	}
	if nested["plain"] != "world" {
		t.Errorf("nested.plain = %v, want world", nested["plain"])
	}
}

// stringerWithToken renders via String() to a value containing a credential,
// exercising the fmt.Stringer redaction branch.
type stringerWithToken string

func (s stringerWithToken) String() string { return "token=" + string(s) }

// A credential embedded in an error (the dominant slog.Any("err", err) shape)
// or a fmt.Stringer must not leak, while unrelated text is preserved unchanged.
func TestHandlerRedactsErrorAndStringerValues(t *testing.T) {
	const jwt = "eyJ0ZXN0IjoxfQ.eyJzdWIiOiJ4In0.c2lnbmF0dXJl"

	var buf bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))
	logger.InfoContext(context.Background(), "verify failed",
		slog.Any("err", fmt.Errorf("verifying token %s: invalid", jwt)),
		slog.Any("stringer", stringerWithToken(jwt)),
		slog.Any("plain", errors.New("connection refused")),
	)

	line := buf.String()
	if strings.Contains(line, jwt) {
		t.Errorf("error or stringer value leaked JWT: %s", line)
	}
	if !strings.Contains(line, redacted) {
		t.Errorf("expected %q in output: %s", redacted, line)
	}
	if !strings.Contains(line, "token=") {
		t.Errorf("stringer value should still render (redacted): %s", line)
	}
	if !strings.Contains(line, "connection refused") {
		t.Errorf("unrelated error message should be preserved: %s", line)
	}
}

func TestRedactStructTags(t *testing.T) {
	type nested struct {
		Token string `json:"token" log:"redact"`
		Name  string `json:"name"`
	}
	type sample struct {
		Username string  `json:"username"`
		Password string  `json:"password" log:"redact"`
		Nested   *nested `json:"nested"`
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.InfoContext(context.Background(), "sample", slog.Attr{
		Key: "sample",
		Value: Redact(sample{
			Username: "alice",
			Password: "p@ssw0rd",
			Nested: &nested{
				Token: "nested-secret",
				Name:  "worker",
			},
		}),
	})

	line := buf.String()
	for _, secret := range []string{"p@ssw0rd", "nested-secret"} {
		if strings.Contains(line, secret) {
			t.Errorf("log output contains secret %q: %s", secret, line)
		}
	}
	for _, want := range []string{"alice", "worker", redacted} {
		if !strings.Contains(line, want) {
			t.Errorf("log output does not contain %q: %s", want, line)
		}
	}
}

func TestRedactNilPointer(t *testing.T) {
	type sample struct {
		Password string `log:"redact"`
	}
	var value *sample

	got := Redact(value).Resolve()
	if got.Kind() != slog.KindAny || got.Any() != nil {
		t.Errorf("Redact(nil pointer) = %#v, want nil any value", got)
	}
}
