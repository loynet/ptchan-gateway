package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultWebhookMaxBodyBytes = 1 << 20
	DefaultWebhookClockSkew    = 5 * time.Minute
)

var (
	ErrWebhookAuthentication = errors.New("ptchan gateway webhook authentication failed")
	ErrWebhookBodyTooLarge   = errors.New("ptchan gateway webhook body exceeds configured limit")
)

type webhookVerificationOptions struct {
	maxBodyBytes int
	maxClockSkew time.Duration
	now          func() time.Time
}

// WebhookVerificationOption configures webhook verification.
type WebhookVerificationOption func(*webhookVerificationOptions) error

// WithWebhookMaxBodyBytes sets the maximum accepted raw webhook body size.
func WithWebhookMaxBodyBytes(limit int) WebhookVerificationOption {
	return func(options *webhookVerificationOptions) error {
		if limit <= 0 {
			return fmt.Errorf("ptchan gateway webhook: body size limit must be positive")
		}
		options.maxBodyBytes = limit
		return nil
	}
}

// WithWebhookClockSkew sets the accepted difference between gateway and local time.
func WithWebhookClockSkew(skew time.Duration) WebhookVerificationOption {
	return func(options *webhookVerificationOptions) error {
		if skew < 0 {
			return fmt.Errorf("ptchan gateway webhook: clock skew must be non-negative")
		}
		options.maxClockSkew = skew
		return nil
	}
}

func withWebhookClock(now func() time.Time) WebhookVerificationOption {
	return func(options *webhookVerificationOptions) error {
		if now == nil {
			return fmt.Errorf("ptchan gateway webhook: clock must not be nil")
		}
		options.now = now
		return nil
	}
}

// VerifyWebhookBody verifies raw webhook bytes and decodes a v1 event.
func VerifyWebhookBody(secret, eventID, timestamp, gotSignature string, body []byte, configure ...WebhookVerificationOption) (*WebhookEvent, error) {
	if strings.TrimSpace(secret) == "" {
		return nil, fmt.Errorf("%w: secret is empty", ErrWebhookAuthentication)
	}
	options := webhookVerificationOptions{
		maxBodyBytes: DefaultWebhookMaxBodyBytes,
		maxClockSkew: DefaultWebhookClockSkew,
		now:          time.Now,
	}
	for _, option := range configure {
		if option == nil {
			return nil, fmt.Errorf("ptchan gateway webhook: verification option must not be nil")
		}
		if err := option(&options); err != nil {
			return nil, err
		}
	}
	if len(body) > options.maxBodyBytes {
		return nil, ErrWebhookBodyTooLarge
	}
	if eventID == "" || timestamp == "" || gotSignature == "" {
		return nil, fmt.Errorf("%w: missing signature headers", ErrWebhookAuthentication)
	}
	observed, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid timestamp", ErrWebhookAuthentication)
	}
	if delta := options.now().Sub(observed); delta > options.maxClockSkew || delta < -options.maxClockSkew {
		return nil, fmt.Errorf("%w: timestamp is outside allowed skew", ErrWebhookAuthentication)
	}
	if !validSignature(gotSignature, webhookSignature(secret, timestamp, body)) {
		return nil, fmt.Errorf("%w: invalid signature", ErrWebhookAuthentication)
	}
	event, err := decodeWebhookEvent(body)
	if err != nil {
		return nil, err
	}
	if event.EventID != eventID {
		return nil, fmt.Errorf("ptchan gateway webhook: event ID header does not match body")
	}
	return event, nil
}

func decodeWebhookEvent(body []byte) (*WebhookEvent, error) {
	var event WebhookEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return nil, fmt.Errorf("ptchan gateway webhook: decode event: %w", err)
	}
	if event.EventID == "" || event.Kind == "" || event.Source == "" || event.ObservedAt.IsZero() {
		return nil, fmt.Errorf("ptchan gateway webhook: event_id, kind, source, and observed_at are required")
	}
	if event.SchemaVersion != SchemaV1 {
		return nil, fmt.Errorf("ptchan gateway webhook: unsupported schema_version %q", event.SchemaVersion)
	}
	if err := validatePost(event.Post); err != nil {
		return nil, fmt.Errorf("ptchan gateway webhook: invalid post: %w", err)
	}
	return &event, nil
}
