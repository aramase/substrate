// Copyright 2026 Anish Ramasekar. Apache-2.0.

package storagebroker

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestS3PresignPost_StartsWith checks whether one presigned POST policy, scoped
// with a starts-with condition on the key, authorizes uploading several
// distinct objects under a prefix -- the S3 analog of an Azure container SAS
// write, and the answer to "how does the prefix (unknown file set) write work
// on S3 without new deps". Uses the already-vendored aws-sdk-go-v2.
//
// Set RUSTFS_ENDPOINT (e.g. http://localhost:9000) to run against the in-cluster
// S3 store.
func TestS3PresignPost_StartsWith(t *testing.T) {
	endpoint := os.Getenv("RUSTFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set RUSTFS_ENDPOINT to run the S3 presigned-POST validation")
	}
	bucket := getenvOr("RUSTFS_BUCKET", "ate-snapshots")
	ak := getenvOr("AWS_ACCESS_KEY_ID", "rustfsadmin")
	sk := getenvOr("AWS_SECRET_ACCESS_KEY", "rustfsadmin")

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(ak, sk, ""),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	presign := s3.NewPresignClient(client)

	ctx := context.Background()
	prefix := "poc-s3post/" + time.Now().Format("20060102T150405Z") + "/"

	// ONE presigned POST, scoped to the prefix. The control plane mints this
	// without knowing the file names.
	post, err := presign.PresignPostObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
	}, func(o *s3.PresignPostOptions) {
		o.Conditions = []any{[]any{"starts-with", "$key", prefix}}
		o.Expires = 15 * time.Minute
	})
	if err != nil {
		t.Fatalf("PresignPostObject: %v", err)
	}
	t.Logf("minted one presigned POST for prefix %q with %d policy fields", prefix, len(post.Values))

	// Upload several distinct objects under the prefix, reusing the one policy,
	// varying only the key -- exactly what atelet would do for the ~5 snapshot
	// files it produces at checkpoint.
	files := map[string][]byte{
		prefix + "manifest.json":  []byte(`{"files":["pages","fs"]}`),
		prefix + "pages.img.zstd": bytes.Repeat([]byte("p"), 4096),
		prefix + "fs.img.zstd":    bytes.Repeat([]byte("f"), 8192),
	}
	for key, data := range files {
		postOneObject(t, post.URL, post.Values, key, data)
	}

	// Read them back to confirm they landed.
	for key, want := range files {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("GetObject %s: %v", key, err)
		}
		got, _ := io.ReadAll(out.Body)
		out.Body.Close()
		if !bytes.Equal(got, want) {
			t.Errorf("%s: read %d bytes, want %d", key, len(got), len(want))
		}
	}
	t.Logf("PASS: one presigned POST policy uploaded %d distinct objects under the prefix", len(files))
}

// postOneObject sends one multipart/form-data POST, reusing the minted policy
// fields and setting key to the target object under the prefix.
func postOneObject(t *testing.T, url string, fields map[string]string, key string, content []byte) {
	t.Helper()
	var buf bytes.Buffer
	form := multipart.NewWriter(&buf)
	for k, v := range fields {
		if k == "key" {
			continue // overridden below
		}
		_ = form.WriteField(k, v)
	}
	_ = form.WriteField("key", key)
	fw, err := form.CreateFormFile("file", "file")
	if err != nil {
		t.Fatal(err)
	}
	fw.Write(content)
	form.Close()

	resp, err := http.Post(url, form.FormDataContentType(), &buf)
	if err != nil {
		t.Fatalf("POST %s: %v", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		t.Fatalf("POST %s: status %d: %s", key, resp.StatusCode, string(body))
	}
}

func getenvOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
