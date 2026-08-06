package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewValidatesInputs(t *testing.T) {
	for _, test := range []struct {
		name        string
		baseURL     string
		credentials Credentials
	}{
		{"empty URL", "", Credentials{Name: "example", Secret: "secret"}},
		{"relative URL", "/gateway", Credentials{Name: "example", Secret: "secret"}},
		{"base path", "https://gateway.example/api", Credentials{Name: "example", Secret: "secret"}},
		{"empty name", "https://gateway.example", Credentials{Secret: "secret"}},
		{"empty secret", "https://gateway.example", Credentials{Name: "example"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.baseURL, test.credentials); err == nil {
				t.Fatal("New() error = nil")
			}
		})
	}
}

func TestReadThreadUsesCanonicalFixtureAndSignsExactPath(t *testing.T) {
	body := fixture(t, "thread.json")
	var request *http.Request
	client := testClient(t, func(req *http.Request) (*http.Response, error) {
		request = req
		return response(http.StatusOK, body), nil
	})

	thread, err := client.ReadThread(context.Background(), ThreadRef{Board: "test", ThreadID: 397}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if thread.Posts[0].Origin == nil || thread.Posts[0].Origin.Name != "example" {
		t.Fatalf("thread post origin = %+v", thread.Posts[0].Origin)
	}
	if request.URL.RequestURI() != "/integration/v1/threads/test/397?limit=50" {
		t.Fatalf("request URI = %q", request.URL.RequestURI())
	}
	timestamp := request.Header.Get(headerTimestamp)
	if timestamp != "2026-07-19T12:00:00Z" {
		t.Fatalf("timestamp = %q", timestamp)
	}
	want := signature("secret", timestamp, http.MethodGet, request.URL.RequestURI(), nil)
	if request.Header.Get(headerIntegration) != "example" || request.Header.Get(headerSignature) != want {
		t.Fatalf("signed headers = %v", request.Header)
	}
}

func TestReadThreadAcceptsAdditiveFields(t *testing.T) {
	client := testClient(t, func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, []byte(`{"board":"test","thread_id":397,"posts":[],"truncated":false,"future_field":{"enabled":true}}`)), nil
	})
	if _, err := client.ReadThread(context.Background(), ThreadRef{Board: "test", ThreadID: 397}, 0); err != nil {
		t.Fatalf("ReadThread() error = %v", err)
	}
}

func TestPostReplySignsExactJSONAndValidatesOrigin(t *testing.T) {
	body := fixture(t, "reply-response.json")
	var request *http.Request
	var requestBody []byte
	client := testClient(t, func(req *http.Request) (*http.Response, error) {
		request = req
		var err error
		requestBody, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		return response(http.StatusOK, body), nil
	})

	if _, err := client.PostReply(context.Background(), ThreadRef{Board: "test", ThreadID: 397}, ">>397\nhello", false); err != nil {
		t.Fatal(err)
	}
	var requestPayload struct {
		Message string `json:"message"`
		Sage    bool   `json:"sage"`
	}
	if err := json.Unmarshal(requestBody, &requestPayload); err != nil {
		t.Fatal(err)
	}
	if requestPayload.Message != ">>397\nhello" || requestPayload.Sage {
		t.Fatalf("request payload = %+v", requestPayload)
	}
	timestamp := request.Header.Get(headerTimestamp)
	want := signature("secret", timestamp, http.MethodPost, request.URL.RequestURI(), requestBody)
	if request.Header.Get("content-type") != "application/json" || request.Header.Get(headerSignature) != want {
		t.Fatalf("request headers = %v", request.Header)
	}
}

func TestGatewayErrorDoesNotRetainResponseBody(t *testing.T) {
	client := testClient(t, func(*http.Request) (*http.Response, error) {
		return response(http.StatusTooManyRequests, []byte(`{"error":{"code":"rate_limited","message":"slow down","retryable":true}}`)), nil
	})
	_, err := client.ReadThread(context.Background(), ThreadRef{Board: "test", ThreadID: 397}, 0)
	var gatewayError *Error
	if !errors.As(err, &gatewayError) {
		t.Fatalf("error = %T %v, want *Error", err, err)
	}
	if gatewayError.StatusCode != http.StatusTooManyRequests || gatewayError.Code != "rate_limited" || !gatewayError.Retryable {
		t.Fatalf("gateway error = %+v", gatewayError)
	}
}

func TestMalformedGatewayErrorIsNotTyped(t *testing.T) {
	client := testClient(t, func(*http.Request) (*http.Response, error) {
		return response(http.StatusBadGateway, []byte(`upstream failure`)), nil
	})
	_, err := client.ReadThread(context.Background(), ThreadRef{Board: "test", ThreadID: 397}, 0)
	var gatewayError *Error
	if errors.As(err, &gatewayError) {
		t.Fatalf("error = %T %v, must not be a gateway envelope", err, err)
	}
}

func TestIncompleteGatewayErrorIsNotTyped(t *testing.T) {
	client := testClient(t, func(*http.Request) (*http.Response, error) {
		return response(http.StatusBadGateway, []byte(`{"error":{"code":"upstream_unavailable","message":"retry later"}}`)), nil
	})
	_, err := client.ReadThread(context.Background(), ThreadRef{Board: "test", ThreadID: 397}, 0)
	var gatewayError *Error
	if errors.As(err, &gatewayError) {
		t.Fatalf("error = %T %v, must not be an incomplete gateway envelope", err, err)
	}
}

func TestClientRejectsMalformedAndOversizedResponses(t *testing.T) {
	t.Run("missing required post field", func(t *testing.T) {
		client := testClient(t, func(*http.Request) (*http.Response, error) {
			return response(http.StatusOK, []byte(`{"board":"test","thread_id":397,"posts":[{"board":"test","thread_id":397,"post_id":397,"url":"https://ptchan.org/test/thread/397.html#397","date":"2026-07-19T12:00:00Z"}],"truncated":false}`)), nil
		})
		if _, err := client.ReadThread(context.Background(), ThreadRef{Board: "test", ThreadID: 397}, 0); err == nil {
			t.Fatal("ReadThread() error = nil")
		}
	})
	t.Run("oversized", func(t *testing.T) {
		client := testClient(t, func(*http.Request) (*http.Response, error) {
			return response(http.StatusOK, []byte(strings.Repeat("x", int(defaultMaxResponseBytes)+1))), nil
		})
		_, err := client.ReadThread(context.Background(), ThreadRef{Board: "test", ThreadID: 397}, 0)
		if !errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("ReadThread() error = %v, want ErrResponseTooLarge", err)
		}
	})
}

func TestVerifyWebhookBodyUsesCanonicalFixtureAndLimits(t *testing.T) {
	body := fixture(t, "webhook-event.json")
	timestamp := "2026-07-19T12:00:00Z"
	options := withWebhookClock(func() time.Time {
		return time.Date(2026, time.July, 19, 12, 0, 30, 0, time.UTC)
	})
	event, err := VerifyWebhookBody("secret", "ptchan:post.created:test:399", timestamp, webhookSignature("secret", timestamp, body), body, options)
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != PostCreated || event.Post.PostID != 399 {
		t.Fatalf("event = %+v", event)
	}
	if _, err := VerifyWebhookBody("secret", event.EventID, timestamp, "hmac-sha256=00", body, options); !errors.Is(err, ErrWebhookAuthentication) {
		t.Fatalf("bad signature error = %v", err)
	}
	if _, err := VerifyWebhookBody("secret", event.EventID, timestamp, webhookSignature("secret", timestamp, body), body, WithWebhookMaxBodyBytes(len(body)-1)); !errors.Is(err, ErrWebhookBodyTooLarge) {
		t.Fatalf("oversized body error = %v", err)
	}
	if _, err := VerifyWebhookBody("secret", "ptchan:post.created:test:400", timestamp, webhookSignature("secret", timestamp, body), body, options); err == nil {
		t.Fatal("event ID mismatch error = nil")
	}
	for _, now := range []time.Time{
		time.Date(2026, time.July, 19, 12, 5, 1, 0, time.UTC),
		time.Date(2026, time.July, 19, 11, 54, 59, 0, time.UTC),
	} {
		_, err := VerifyWebhookBody("secret", event.EventID, timestamp, webhookSignature("secret", timestamp, body), body, withWebhookClock(func() time.Time { return now }))
		if !errors.Is(err, ErrWebhookAuthentication) {
			t.Fatalf("timestamp at %s error = %v, want authentication error", now, err)
		}
	}
}

func TestVerifyWebhookBodyAcceptsFutureEventKind(t *testing.T) {
	body := fixture(t, "webhook-event.json")
	var event map[string]any
	if err := json.Unmarshal(body, &event); err != nil {
		t.Fatal(err)
	}
	event["event_id"] = "ptchan:post.updated:test:399"
	event["kind"] = "post.updated"
	event["future_field"] = true
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	timestamp := "2026-07-19T12:00:00Z"
	decoded, err := VerifyWebhookBody(
		"secret",
		"ptchan:post.updated:test:399",
		timestamp,
		webhookSignature("secret", timestamp, body),
		body,
		withWebhookClock(func() time.Time { return time.Date(2026, time.July, 19, 12, 0, 30, 0, time.UTC) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != "post.updated" {
		t.Fatalf("event kind = %q", decoded.Kind)
	}
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "docs", "contract", "examples", name))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func testClient(t *testing.T, roundTrip func(*http.Request) (*http.Response, error)) *Client {
	t.Helper()
	client, err := New(
		"https://gateway.example",
		Credentials{Name: "example", Secret: "secret"},
		WithHTTPClient(&http.Client{Transport: roundTripFunc(roundTrip)}),
		withClock(func() time.Time { return time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func response(status int, body []byte) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(string(body))),
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }
