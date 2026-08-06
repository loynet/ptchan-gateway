// Package gateway implements ptchan-gateway's signed integration protocol.
package gateway

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	headerIntegration = "x-ptchan-integration"
	headerTimestamp   = "x-ptchan-timestamp"
	headerSignature   = "x-ptchan-signature"
)

// Credentials identify one configured gateway integration.
type Credentials struct {
	Name   string
	Secret string
}

func (c Credentials) validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return fmt.Errorf("ptchan gateway: integration name is required")
	}
	if strings.TrimSpace(c.Secret) == "" {
		return fmt.Errorf("ptchan gateway: integration secret is required")
	}
	return nil
}

// ThreadRef identifies a thread by board and OP post ID.
type ThreadRef struct {
	Board    string
	ThreadID int64
}

func (r ThreadRef) validate() error {
	if strings.TrimSpace(r.Board) == "" || r.ThreadID <= 0 {
		return fmt.Errorf("board and positive thread_id are required")
	}
	return nil
}

func (r ThreadRef) path() string {
	return fmt.Sprintf("/integration/v1/threads/%s/%d", url.PathEscape(r.Board), r.ThreadID)
}

type EventKind string

const (
	ThreadCreated EventKind = "thread.created"
	PostCreated   EventKind = "post.created"
)

type SchemaVersion string

const SchemaV1 SchemaVersion = "1"

type OriginKind string

const IntegrationOrigin OriginKind = "integration"

// WebhookEvent is a v1 event delivered by the gateway.
type WebhookEvent struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	EventID       string        `json:"event_id"`
	Kind          EventKind     `json:"kind"`
	Source        string        `json:"source"`
	ObservedAt    time.Time     `json:"observed_at"`
	Post          Post          `json:"post"`
}

func (e *WebhookEvent) UnmarshalJSON(data []byte) error {
	type wire WebhookEvent
	if err := requireFields(data, "schema_version", "event_id", "kind", "source", "observed_at", "post"); err != nil {
		return err
	}
	return json.Unmarshal(data, (*wire)(e))
}

// Thread is a sanitized gateway thread response.
type Thread struct {
	Board     string `json:"board"`
	ThreadID  int64  `json:"thread_id"`
	Posts     []Post `json:"posts"`
	Truncated bool   `json:"truncated"`
}

func (t Thread) ThreadRef() ThreadRef { return ThreadRef{Board: t.Board, ThreadID: t.ThreadID} }

func (t *Thread) UnmarshalJSON(data []byte) error {
	type wire Thread
	if err := requireFields(data, "board", "thread_id", "posts", "truncated"); err != nil {
		return err
	}
	return json.Unmarshal(data, (*wire)(t))
}

// ReplyResponse identifies a reply accepted by ptchan.
type ReplyResponse struct {
	Board    string     `json:"board"`
	ThreadID int64      `json:"thread_id"`
	PostID   int64      `json:"post_id"`
	URL      string     `json:"url"`
	Origin   PostOrigin `json:"origin"`
}

func (r *ReplyResponse) UnmarshalJSON(data []byte) error {
	type wire ReplyResponse
	if err := requireFields(data, "board", "thread_id", "post_id", "url", "origin"); err != nil {
		return err
	}
	return json.Unmarshal(data, (*wire)(r))
}

// Post is the moderation-safe post representation in the integration contract.
type Post struct {
	Board             string      `json:"board"`
	ThreadID          int64       `json:"thread_id"`
	PostID            int64       `json:"post_id"`
	URL               string      `json:"url"`
	Date              time.Time   `json:"date"`
	Subject           string      `json:"subject,omitempty"`
	Message           string      `json:"message,omitempty"`
	Name              string      `json:"name,omitempty"`
	Tripcode          string      `json:"tripcode,omitempty"`
	Capcode           string      `json:"capcode,omitempty"`
	Donor             *bool       `json:"donor,omitempty"`
	Country           string      `json:"country,omitempty"`
	PosterFingerprint string      `json:"poster_fingerprint,omitempty"`
	Origin            *PostOrigin `json:"origin,omitempty"`
	AttachmentCount   int64       `json:"attachment_count"`
	References        []PostRef   `json:"references,omitempty"`
	ReferencedBy      []PostRef   `json:"referenced_by,omitempty"`
}

func (p Post) ThreadRef() ThreadRef { return ThreadRef{Board: p.Board, ThreadID: p.ThreadID} }

func (p *Post) UnmarshalJSON(data []byte) error {
	type wire Post
	if err := requireFields(data, "board", "thread_id", "post_id", "url", "date", "attachment_count"); err != nil {
		return err
	}
	return json.Unmarshal(data, (*wire)(p))
}

type PostOrigin struct {
	Kind OriginKind `json:"kind"`
	Name string     `json:"name"`
}

// PostRef contains complete coordinates for a referenced post.
type PostRef struct {
	Board    string `json:"board"`
	ThreadID int64  `json:"thread_id"`
	PostID   int64  `json:"post_id"`
}

func (r PostRef) ThreadRef() ThreadRef { return ThreadRef{Board: r.Board, ThreadID: r.ThreadID} }

func requireFields(data []byte, names ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range names {
		value, ok := fields[name]
		if !ok || string(value) == "null" {
			return fmt.Errorf("required field %q is missing", name)
		}
	}
	return nil
}
