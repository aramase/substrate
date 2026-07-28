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

// Package logredact provides slog helpers for keeping known credential formats
// and explicitly-tagged sensitive fields out of logs.
//
// The Handler redacts credential-shaped strings in messages, string attributes,
// nested groups, and the string form of error and fmt.Stringer values (errors
// are the codebase's dominant log payload). It does NOT reach inside arbitrary
// structs logged via slog.Any; log those through Redact so their fields are
// exposed as redactable attributes and any field tagged log:"redact" is dropped.
package logredact

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"time"
)

const redacted = "REDACTED"

var (
	// JWTs begin with "eyJ" for the base64url-encoded JSON header. Keep this
	// specific to compact JWTs to avoid redacting ordinary dotted strings.
	jwtPattern = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`)
	// Redact Bearer credentials while preserving the authentication scheme for
	// log readability.
	bearerPattern = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+`)
	// Redact header-style Authorization values even if the value is not a
	// Bearer token.
	authorizationPattern = regexp.MustCompile(`(?i)(\bauthorization\s*[:=]\s*)(?:Bearer\s+)?[^\s,;]+`)
	// Private keys can contain newlines, so this pattern intentionally spans
	// across lines and replaces the whole PEM block.
	privateKeyPattern = regexp.MustCompile(`(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----`)
)

type replacer struct {
	pattern     *regexp.Regexp
	replacement string
}

var stringReplacers = []replacer{
	{pattern: privateKeyPattern, replacement: redacted},
	{pattern: authorizationPattern, replacement: `${1}` + redacted},
	{pattern: bearerPattern, replacement: `Bearer ` + redacted},
	{pattern: jwtPattern, replacement: redacted},
}

// Handler redacts known credential formats from slog messages and string attrs
// before forwarding records to an inner handler.
type Handler struct {
	internal slog.Handler
}

// NewHandler returns a slog.Handler that redacts sensitive log content before
// passing records to internal.
func NewHandler(internal slog.Handler) *Handler {
	return &Handler{
		internal: internal,
	}
}

func (h *Handler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.internal.Enabled(ctx, lvl)
}

func (h *Handler) Handle(ctx context.Context, rec slog.Record) error {
	message, messageChanged := redactString(rec.Message)
	attrs, attrsChanged := redactRecordAttrs(rec)
	if attrsChanged {
		redactedRecord := slog.NewRecord(rec.Time, rec.Level, message, rec.PC)
		redactedRecord.AddAttrs(attrs...)
		rec = redactedRecord
	} else if messageChanged {
		rec.Message = message
	}
	return h.internal.Handle(ctx, rec)
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	redactedAttrs, _ := redactAttrs(attrs)
	return &Handler{internal: h.internal.WithAttrs(redactedAttrs)}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{internal: h.internal.WithGroup(name)}
}

func redactRecordAttrs(rec slog.Record) ([]slog.Attr, bool) {
	changed := false
	rec.Attrs(func(attr slog.Attr) bool {
		_, attrChanged := redactAttr(attr)
		if attrChanged {
			changed = true
			return false
		}
		return true
	})
	if !changed {
		return nil, false
	}

	var redactedAttrs []slog.Attr
	rec.Attrs(func(attr slog.Attr) bool {
		redactedAttr, _ := redactAttr(attr)
		redactedAttrs = append(redactedAttrs, redactedAttr)
		return true
	})
	return redactedAttrs, true
}

func redactAttrs(attrs []slog.Attr) ([]slog.Attr, bool) {
	var redactedAttrs []slog.Attr
	changed := false
	for _, attr := range attrs {
		redactedAttr, attrChanged := redactAttr(attr)
		if attrChanged {
			changed = true
		}
		redactedAttrs = append(redactedAttrs, redactedAttr)
	}
	if !changed {
		return attrs, false
	}
	return redactedAttrs, true
}

func redactAttr(attr slog.Attr) (slog.Attr, bool) {
	value, changed := redactValue(attr.Value)
	if changed {
		attr.Value = value
	}
	return attr, changed
}

func redactValue(value slog.Value) (slog.Value, bool) {
	value = value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		redactedString, changed := redactString(value.String())
		if !changed {
			return value, false
		}
		return slog.StringValue(redactedString), true
	case slog.KindGroup:
		groupAttrs := value.Group()
		redactedAttrs, changed := redactAttrs(groupAttrs)
		if !changed {
			return value, false
		}
		return slog.GroupValue(redactedAttrs...), true
	case slog.KindAny:
		return redactAnyValue(value)
	default:
		return value, false
	}
}

// redactAnyValue redacts credential strings from values that log as text but
// resolve to KindAny rather than KindString — most importantly errors, which
// are the codebase's dominant log payload (slog.Any("err", err)). A value whose
// string form matches is replaced with the redacted string; anything else is
// left untouched so its normal rendering is preserved.
func redactAnyValue(value slog.Value) (slog.Value, bool) {
	var text string
	switch v := value.Any().(type) {
	case error:
		text = v.Error()
	case fmt.Stringer:
		text = v.String()
	default:
		return value, false
	}
	redactedText, changed := redactString(text)
	if !changed {
		return value, false
	}
	return slog.StringValue(redactedText), true
}

func redactString(value string) (string, bool) {
	redactedValue := value
	for _, replacer := range stringReplacers {
		redactedValue = replacer.pattern.ReplaceAllString(redactedValue, replacer.replacement)
	}
	return redactedValue, redactedValue != value
}

// Redact returns a slog.Value that renders v with struct fields tagged
// `log:"redact"` replaced by REDACTED. Structs nested behind pointers are
// traversed; nil pointers and unexported fields are handled without panicking.
func Redact(v any) slog.Value {
	return slog.AnyValue(redactedValue{value: v})
}

type redactedValue struct {
	value any
}

func (v redactedValue) LogValue() slog.Value {
	return redactedLogValue(reflect.ValueOf(v.value), true)
}

func redactedLogValue(value reflect.Value, forceGroup bool) slog.Value {
	if !value.IsValid() {
		return slog.AnyValue(nil)
	}
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return slog.AnyValue(nil)
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct || isScalarStruct(value.Type()) {
		if value.CanInterface() {
			return slog.AnyValue(value.Interface())
		}
		return slog.AnyValue(nil)
	}

	attrs, changed := redactStructAttrs(value)
	if forceGroup || changed {
		return slog.GroupValue(attrs...)
	}
	if value.CanInterface() {
		return slog.AnyValue(value.Interface())
	}
	return slog.GroupValue(attrs...)
}

func redactStructAttrs(value reflect.Value) ([]slog.Attr, bool) {
	valueType := value.Type()
	attrs := make([]slog.Attr, 0, value.NumField())
	changed := false
	for i := 0; i < value.NumField(); i++ {
		field := valueType.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name, skip := logFieldName(field)
		if skip {
			continue
		}
		fieldValue := value.Field(i)
		if shouldRedactField(field) {
			attrs = append(attrs, slog.String(name, redacted))
			changed = true
			continue
		}
		attrValue := redactedLogValue(fieldValue, false)
		if attrValue.Kind() == slog.KindGroup {
			changed = true
		}
		attrs = append(attrs, slog.Attr{Key: name, Value: attrValue})
	}
	return attrs, changed
}

func logFieldName(field reflect.StructField) (string, bool) {
	if tag := field.Tag.Get("json"); tag != "" {
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" {
			return "", true
		}
		if name != "" {
			return name, false
		}
	}
	return field.Name, false
}

func shouldRedactField(field reflect.StructField) bool {
	for tag := range strings.SplitSeq(field.Tag.Get("log"), ",") {
		if tag == "redact" {
			return true
		}
	}
	return false
}

func isScalarStruct(valueType reflect.Type) bool {
	return valueType == reflect.TypeFor[time.Time]()
}
