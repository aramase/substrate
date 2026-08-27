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

// This file is the in-tree S3 storage broker. Unlike Azure, S3's SDK is already
// vendored for atelet's object store, so the broker registers in-process with no
// new dependency -- the "adopt now" path for the backend substrate ships today.
// It mints signed access using two S3 mechanisms:
//   - write: one presigned POST policy with a starts-with condition on the key,
//     so a single capability covers every file atelet writes under the snapshot
//     prefix without the control plane knowing the file names in advance, and
//   - read: a presigned GET per object, enumerated by listing the prefix (the
//     broker holds the credential; the node never does).

package storagebroker

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func init() {
	Register("s3-signed", newS3Broker)
}

type s3Broker struct {
	client    *s3.Client
	presign   *s3.PresignClient
	bucket    string
	prefixURL string
}

func newS3Broker(ctx context.Context) (Broker, error) {
	endpoint := firstEnv("S3_ENDPOINT", "AWS_ENDPOINT_URL")
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		return nil, fmt.Errorf("s3 storage broker needs S3_BUCKET")
	}
	region := firstEnv("AWS_REGION", "AWS_DEFAULT_REGION")
	if region == "" {
		region = "us-east-1"
	}
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), ""),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	prefixURL := strings.TrimRight(endpoint, "/") + "/" + bucket
	return &s3Broker{
		client:    client,
		presign:   s3.NewPresignClient(client),
		bucket:    bucket,
		prefixURL: prefixURL,
	}, nil
}

// snapshotKeyPrefix turns a snapshot URI (e.g. "gs://bucket/ns/actor/snap" or
// "s3://bucket/ns/actor/snap") into the object-key prefix atelet's file keys
// share, mirroring how atelet's parseGCSURL derives the object path.
func snapshotKeyPrefix(snapshotURI string) string {
	u, err := url.Parse(snapshotURI)
	if err != nil {
		return strings.Trim(snapshotURI, "/")
	}
	return strings.Trim(u.Path, "/")
}

// MintWrite mints one presigned POST policy scoped to the snapshot prefix with a
// starts-with condition, so atelet uploads every file it produces under the
// prefix with the same signed fields.
func (b *s3Broker) MintWrite(ctx context.Context, snapshotURI string, ttl time.Duration) (Capability, error) {
	prefix := snapshotKeyPrefix(snapshotURI) + "/"
	post, err := b.presign.PresignPostObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(prefix),
	}, func(o *s3.PresignPostOptions) {
		o.Conditions = []any{[]any{"starts-with", "$key", prefix}}
		o.Expires = ttl
	})
	if err != nil {
		return Capability{}, fmt.Errorf("presigning S3 POST policy: %w", err)
	}
	return Capability{
		PrefixURL:   b.prefixURL,
		WriteMethod: WriteMethodPOST,
		PostURL:     post.URL,
		PostFields:  post.Values,
	}, nil
}

// MintRead lists the snapshot prefix and mints a presigned GET per object. The
// broker holds the credential and does the enumeration, so the control plane
// need not know the file names and the node never gets a credential.
func (b *s3Broker) MintRead(ctx context.Context, snapshotURI string, ttl time.Duration) (Capability, error) {
	prefix := snapshotKeyPrefix(snapshotURI) + "/"
	out, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return Capability{}, fmt.Errorf("listing snapshot prefix %q: %w", prefix, err)
	}
	urls := make(map[string]string, len(out.Contents))
	for _, obj := range out.Contents {
		key := aws.ToString(obj.Key)
		pres, err := b.presign.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(key),
		}, s3.WithPresignExpires(ttl))
		if err != nil {
			return Capability{}, fmt.Errorf("presigning GET for %q: %w", key, err)
		}
		urls[key] = pres.URL
	}
	return Capability{PrefixURL: b.prefixURL, ReadObjectURLs: urls}, nil
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}
