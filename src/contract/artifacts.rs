use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use utoipa::openapi::OpenApi;

use super::{
    ErrorBody, ErrorCode, ErrorEnvelope, EventKind, OriginKind, Post, PostOrigin, ReplyRequest,
    ReplyResponse, SchemaVersion, Thread, WebhookEvent,
};

const ARTIFACT_DIR: &str = "docs/contract";

struct Artifact {
    path: &'static str,
    body: String,
}

pub(crate) fn default_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(ARTIFACT_DIR)
}

pub(crate) fn write(openapi: &OpenApi) -> Result<()> {
    let root = default_dir();
    for artifact in render(openapi)? {
        let path = root.join(artifact.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create contract directory {}", parent.display()))?;
        }
        fs::write(&path, artifact.body)
            .with_context(|| format!("write contract artifact {}", path.display()))?;
    }
    Ok(())
}

pub(crate) fn check(openapi: &OpenApi) -> Result<()> {
    let root = default_dir();
    for artifact in render(openapi)? {
        let path = root.join(artifact.path);
        let committed = fs::read_to_string(&path)
            .with_context(|| format!("read contract artifact {}", path.display()))?;
        if committed != artifact.body {
            anyhow::bail!(
                "contract artifact {} is stale; run cargo run -- --write-contract",
                path.display()
            );
        }
    }
    Ok(())
}

fn render(openapi: &OpenApi) -> Result<Vec<Artifact>> {
    let fixtures = fixtures()?;
    Ok(vec![
        artifact("openapi.json", openapi)?,
        schema_artifact::<WebhookEvent>("schemas/webhook-event.schema.json")?,
        schema_artifact::<Thread>("schemas/thread.schema.json")?,
        schema_artifact::<ReplyRequest>("schemas/reply-request.schema.json")?,
        schema_artifact::<ReplyResponse>("schemas/reply-response.schema.json")?,
        schema_artifact::<ErrorEnvelope>("schemas/error.schema.json")?,
        artifact("examples/webhook-event.json", &fixtures.webhook_event)?,
        artifact("examples/thread.json", &fixtures.thread)?,
        artifact("examples/reply-request.json", &fixtures.reply_request)?,
        artifact("examples/reply-response.json", &fixtures.reply_response)?,
        artifact("examples/error.json", &fixtures.error)?,
    ])
}

fn schema_artifact<T: JsonSchema>(path: &'static str) -> Result<Artifact> {
    artifact(path, &schema_for!(T))
}

fn artifact(path: &'static str, value: &impl Serialize) -> Result<Artifact> {
    let mut body = serde_json::to_string_pretty(value).context("encode contract artifact")?;
    body.push('\n');
    Ok(Artifact { path, body })
}

struct Fixtures {
    webhook_event: WebhookEvent,
    thread: Thread,
    reply_request: ReplyRequest,
    reply_response: ReplyResponse,
    error: ErrorEnvelope,
}

fn fixtures() -> Result<Fixtures> {
    let observed_at = timestamp("2026-07-19T12:00:02Z")?;
    let post = Post {
        board: "test".to_string(),
        thread_id: 397,
        id: 399,
        url: "https://ptchan.org/test/thread/397.html#399".to_string(),
        date: timestamp("2026-07-19T12:00:00Z")?,
        subject: None,
        message: Some(">>397\nhello from the integration".to_string()),
        name: Some("gateway".to_string()),
        tripcode: Some("!!X8NXmAS44=".to_string()),
        capcode: None,
        donor: Some(false),
        country: Some("PT".to_string()),
        poster_fingerprint: None,
        origin: Some(PostOrigin {
            kind: OriginKind::Integration,
            name: "example".to_string(),
        }),
        attachment_count: 0,
        references: Vec::new(),
        referenced_by: Vec::new(),
    };
    Ok(Fixtures {
        webhook_event: WebhookEvent {
            schema_version: SchemaVersion::V1,
            event_id: "ptchan:post.created:test:399".to_string(),
            kind: EventKind::PostCreated,
            source: "ptchan".to_string(),
            observed_at,
            post: post.clone(),
        },
        thread: Thread {
            board: "test".to_string(),
            id: 397,
            posts: vec![post],
            truncated: false,
        },
        reply_request: ReplyRequest {
            message: ">>397\nhello from the integration".to_string(),
            sage: false,
        },
        reply_response: ReplyResponse {
            board: "test".to_string(),
            thread_id: 397,
            post_id: 399,
            url: "https://ptchan.org/test/thread/397.html#399".to_string(),
            origin: PostOrigin {
                kind: OriginKind::Integration,
                name: "example".to_string(),
            },
        },
        error: ErrorEnvelope {
            error: ErrorBody {
                code: ErrorCode::RateLimited,
                message: "gateway rate limit exceeded".to_string(),
                retryable: true,
                upstream_status: None,
            },
        },
    })
}

fn timestamp(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("parse fixture timestamp {value}"))?
        .with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn examples_are_valid_contract_values() {
        let fixtures = fixtures().unwrap();

        let webhook = serde_json::to_vec(&fixtures.webhook_event).unwrap();
        let thread = serde_json::to_vec(&fixtures.thread).unwrap();
        let request = serde_json::to_vec(&fixtures.reply_request).unwrap();
        let response = serde_json::to_vec(&fixtures.reply_response).unwrap();
        let error = serde_json::to_vec(&fixtures.error).unwrap();

        serde_json::from_slice::<WebhookEvent>(&webhook).unwrap();
        serde_json::from_slice::<Thread>(&thread).unwrap();
        serde_json::from_slice::<ReplyRequest>(&request).unwrap();
        serde_json::from_slice::<ReplyResponse>(&response).unwrap();
        serde_json::from_slice::<ErrorEnvelope>(&error).unwrap();
    }
}
