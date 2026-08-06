package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultRequestTimeout         = 15 * time.Second
	defaultMaxResponseBytes int64 = 1 << 20
)

// Client is a signed ptchan-gateway integration API client.
type Client struct {
	baseURL          string
	credentials      Credentials
	httpClient       *http.Client
	maxResponseBytes int64
	now              func() time.Time
}

type clientOptions struct {
	httpClient       *http.Client
	maxResponseBytes int64
	now              func() time.Time
}

// ClientOption configures a Client at construction time.
type ClientOption func(*clientOptions) error

// WithHTTPClient supplies the HTTP transport used by the client. The caller is
// responsible for configuring a timeout on a supplied client.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(options *clientOptions) error {
		if client == nil {
			return fmt.Errorf("ptchan gateway: HTTP client must not be nil")
		}
		options.httpClient = client
		return nil
	}
}

// WithMaxResponseBytes sets the maximum decoded gateway response size.
func WithMaxResponseBytes(limit int64) ClientOption {
	return func(options *clientOptions) error {
		if limit <= 0 {
			return fmt.Errorf("ptchan gateway: response size limit must be positive")
		}
		options.maxResponseBytes = limit
		return nil
	}
}

// withClock makes signed-request timestamps deterministic in package tests.
func withClock(now func() time.Time) ClientOption {
	return func(options *clientOptions) error {
		if now == nil {
			return fmt.Errorf("ptchan gateway: clock must not be nil")
		}
		options.now = now
		return nil
	}
}

// New validates the gateway endpoint and integration credentials.
func New(baseURL string, credentials Credentials, configure ...ClientOption) (*Client, error) {
	if err := credentials.validate(); err != nil {
		return nil, err
	}
	endpoint, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return nil, fmt.Errorf("ptchan gateway: base URL must be an absolute HTTP URL")
	}
	if endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return nil, fmt.Errorf("ptchan gateway: base URL must not include credentials, path, query, or fragment")
	}
	options := clientOptions{
		httpClient:       &http.Client{Timeout: defaultRequestTimeout},
		maxResponseBytes: defaultMaxResponseBytes,
		now:              time.Now,
	}
	for _, option := range configure {
		if option == nil {
			return nil, fmt.Errorf("ptchan gateway: client option must not be nil")
		}
		if err := option(&options); err != nil {
			return nil, err
		}
	}
	return &Client{
		baseURL:          strings.TrimRight(endpoint.String(), "/"),
		credentials:      credentials,
		httpClient:       options.httpClient,
		maxResponseBytes: options.maxResponseBytes,
		now:              options.now,
	}, nil
}

// ReadThread returns the requested sanitized thread. A non-positive limit lets
// the gateway apply its documented default.
func (c *Client) ReadThread(ctx context.Context, ref ThreadRef, limit int) (*Thread, error) {
	if err := ref.validate(); err != nil {
		return nil, fmt.Errorf("ptchan gateway: read thread: %w", err)
	}
	path := ref.path()
	if limit > 0 {
		path += "?limit=" + strconv.Itoa(limit)
	}
	var thread Thread
	if err := c.do(ctx, http.MethodGet, path, nil, &thread); err != nil {
		return nil, err
	}
	if err := validateThread(thread, ref); err != nil {
		return nil, fmt.Errorf("ptchan gateway: invalid thread response: %w", err)
	}
	return &thread, nil
}

// PostReply submits one reply to an existing thread. It does not retry posting
// because an upstream timeout can leave the reply state unknown.
func (c *Client) PostReply(ctx context.Context, ref ThreadRef, message string, sage bool) (*ReplyResponse, error) {
	if err := ref.validate(); err != nil {
		return nil, fmt.Errorf("ptchan gateway: post reply: %w", err)
	}
	body, err := json.Marshal(struct {
		Message string `json:"message"`
		Sage    bool   `json:"sage"`
	}{Message: message, Sage: sage})
	if err != nil {
		return nil, fmt.Errorf("ptchan gateway: encode reply: %w", err)
	}
	var reply ReplyResponse
	if err := c.do(ctx, http.MethodPost, ref.path()+"/replies", body, &reply); err != nil {
		return nil, err
	}
	if err := validateReplyResponse(reply, ref, c.credentials.Name); err != nil {
		return nil, fmt.Errorf("ptchan gateway: invalid reply response: %w", err)
	}
	return &reply, nil
}

// Error is the gateway's documented JSON error envelope.
type Error struct {
	StatusCode     int    `json:"-"`
	Code           string `json:"code"`
	Message        string `json:"message"`
	Retryable      bool   `json:"retryable"`
	UpstreamStatus int    `json:"upstream_status,omitempty"`
}

func (e *Error) Error() string {
	if e.Code != "" && e.Message != "" {
		return fmt.Sprintf("ptchan gateway: status %d: %s: %s", e.StatusCode, e.Code, e.Message)
	}
	if e.Code != "" {
		return fmt.Sprintf("ptchan gateway: status %d: %s", e.StatusCode, e.Code)
	}
	return fmt.Sprintf("ptchan gateway: status %d", e.StatusCode)
}

var ErrResponseTooLarge = errors.New("ptchan gateway: response exceeds configured limit")

func (c *Client) do(ctx context.Context, method, path string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("ptchan gateway: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	timestamp := c.now().UTC().Format(time.RFC3339)
	req.Header.Set(headerIntegration, c.credentials.Name)
	req.Header.Set(headerTimestamp, timestamp)
	req.Header.Set(headerSignature, signature(c.credentials.Secret, timestamp, method, path, body))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ptchan gateway: request: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, c.maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("ptchan gateway: read response: %w", err)
	}
	if int64(len(respBody)) > c.maxResponseBytes {
		return ErrResponseTooLarge
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return decodeError(resp.StatusCode, respBody)
	}
	if out == nil || len(respBody) == 0 {
		return nil
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("ptchan gateway: decode response: %w", err)
	}
	return nil
}

func decodeError(statusCode int, body []byte) error {
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil || requireFields(envelope.Error, "code", "message", "retryable") != nil {
		return fmt.Errorf("ptchan gateway: unexpected HTTP status %d", statusCode)
	}
	var gatewayError Error
	if json.Unmarshal(envelope.Error, &gatewayError) != nil {
		return fmt.Errorf("ptchan gateway: unexpected HTTP status %d", statusCode)
	}
	gatewayError.StatusCode = statusCode
	return &gatewayError
}

func validateThread(thread Thread, requested ThreadRef) error {
	if thread.Board == "" || thread.ThreadID <= 0 || thread.Posts == nil || thread.ThreadRef() != requested {
		return fmt.Errorf("coordinates and posts are required and must match the requested thread")
	}
	for i, post := range thread.Posts {
		if err := validatePost(post); err != nil {
			return fmt.Errorf("posts[%d]: %w", i, err)
		}
		if post.ThreadRef() != requested {
			return fmt.Errorf("posts[%d]: coordinates do not match requested thread", i)
		}
		if i > 0 && post.Date.Before(thread.Posts[i-1].Date) {
			return fmt.Errorf("posts are not chronological")
		}
	}
	return nil
}

func validateReplyResponse(reply ReplyResponse, requested ThreadRef, integrationName string) error {
	if reply.Board == "" || reply.ThreadID <= 0 || reply.PostID <= 0 || reply.URL == "" {
		return fmt.Errorf("board, thread_id, post_id, and url are required")
	}
	if reply.Board != requested.Board || reply.ThreadID != requested.ThreadID {
		return fmt.Errorf("coordinates do not match requested thread")
	}
	if err := validateOrigin(reply.Origin); err != nil {
		return fmt.Errorf("origin: %w", err)
	}
	if reply.Origin.Name != integrationName {
		return fmt.Errorf("origin does not match requesting integration")
	}
	return nil
}

func validatePost(post Post) error {
	if post.Board == "" || post.ThreadID <= 0 || post.PostID <= 0 || post.URL == "" || post.Date.IsZero() || post.AttachmentCount < 0 {
		return fmt.Errorf("board, thread_id, post_id, url, date, and non-negative attachment_count are required")
	}
	if post.Origin != nil {
		if err := validateOrigin(*post.Origin); err != nil {
			return fmt.Errorf("origin: %w", err)
		}
	}
	for _, ref := range append(post.References, post.ReferencedBy...) {
		if ref.Board == "" || ref.ThreadID <= 0 || ref.PostID <= 0 {
			return fmt.Errorf("post references require board, thread_id, and post_id")
		}
	}
	return nil
}

func validateOrigin(origin PostOrigin) error {
	if origin.Kind != IntegrationOrigin || origin.Name == "" {
		return fmt.Errorf("kind must be %q and name is required", IntegrationOrigin)
	}
	return nil
}
