use std::{sync::Arc, time::Instant};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{FromRequestParts, Path},
    http::{request::Parts, uri::PathAndQuery, HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::{
    config::{IntegrationConfig, PostingConfig},
    contract::ErrorCode,
};

use super::{responses::ApiError, telemetry::reject_reading_request, AppState};

type HmacSha256 = Hmac<Sha256>;
const REQUEST_MAX_SKEW_SECONDS: i64 = 5 * 60;

#[derive(Clone)]
pub(super) struct VerifiedReading {
    pub(super) integration: Arc<IntegrationConfig>,
    pub(super) started_at: Instant,
}

pub(super) struct AuthError {
    label: String,
}

impl AuthError {
    pub(super) fn label(&self) -> &str {
        &self.label
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
            "request authentication failed",
        )
        .into_response()
    }
}

impl FromRequestParts<AppState> for VerifiedReading {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> std::result::Result<Self, Self::Rejection> {
        let started_at = Instant::now();
        let Path((board, thread_id)) = Path::<(String, String)>::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        let thread_id = thread_id.parse::<i64>().unwrap_or_default();
        let integration = find_reading_config(state, &parts.headers).and_then(|integration| {
            verify_request_headers(
                &integration.secret,
                &parts.headers,
                &parts.method,
                &parts.uri,
                None,
            )
            .map_err(|_| auth_error(state, &parts.headers))?;
            Ok(integration)
        });
        let integration = match integration {
            Ok(integration) => integration,
            Err(err) => {
                reject_reading_request(
                    err.label(),
                    &board,
                    thread_id,
                    "unauthorized",
                    StatusCode::UNAUTHORIZED,
                    started_at,
                );
                return Err(err.into_response());
            }
        };
        Ok(Self {
            integration,
            started_at,
        })
    }
}

pub(super) fn authenticate_posting(
    state: &AppState,
    headers: &HeaderMap,
    method: &Method,
    uri: &Uri,
    body: &[u8],
) -> std::result::Result<Arc<PostingConfig>, AuthError> {
    let posting = find_posting_config(state, headers)?;
    verify_request_headers(&posting.secret, headers, method, uri, Some(body))
        .map_err(|_| auth_error(state, headers))?;
    Ok(posting)
}

fn find_reading_config(
    state: &AppState,
    headers: &HeaderMap,
) -> std::result::Result<Arc<IntegrationConfig>, AuthError> {
    let name = header(headers, "x-ptchan-integration").ok_or_else(|| auth_error(state, headers))?;
    state
        .integrations
        .get(name)
        .cloned()
        .ok_or_else(|| auth_error(state, headers))
}

fn find_posting_config(
    state: &AppState,
    headers: &HeaderMap,
) -> std::result::Result<Arc<PostingConfig>, AuthError> {
    let name = header(headers, "x-ptchan-integration").ok_or_else(|| auth_error(state, headers))?;
    state
        .postings
        .get(name)
        .cloned()
        .ok_or_else(|| auth_error(state, headers))
}

fn auth_error(state: &AppState, headers: &HeaderMap) -> AuthError {
    AuthError {
        label: requested_integration_label(state, headers).to_string(),
    }
}

fn requested_integration_label<'a>(state: &'a AppState, headers: &'a HeaderMap) -> &'a str {
    let Some(name) = header(headers, "x-ptchan-integration") else {
        return "unknown";
    };
    if state.integrations.contains_key(name) || state.postings.contains_key(name) {
        name
    } else {
        "unknown"
    }
}

fn verify_request_headers(
    secret: &str,
    headers: &HeaderMap,
    method: &Method,
    uri: &Uri,
    body: Option<&[u8]>,
) -> Result<()> {
    let timestamp = header(headers, "x-ptchan-timestamp").context("missing x-ptchan-timestamp")?;
    let parsed_timestamp = DateTime::parse_from_rfc3339(timestamp)
        .context("x-ptchan-timestamp must be RFC3339")?
        .with_timezone(&Utc);
    let skew = (Utc::now() - parsed_timestamp).num_seconds().abs();
    if skew > REQUEST_MAX_SKEW_SECONDS {
        anyhow::bail!("x-ptchan-timestamp is outside allowed skew");
    }
    let signature = header(headers, "x-ptchan-signature").context("missing x-ptchan-signature")?;
    verify_request_signature(secret, timestamp, method, uri, body, signature)
}

pub(super) fn verify_request_signature(
    secret: &str,
    timestamp: &str,
    method: &Method,
    uri: &Uri,
    body: Option<&[u8]>,
    signature: &str,
) -> Result<()> {
    let provided = signature
        .strip_prefix("hmac-sha256=")
        .ok_or_else(|| anyhow!("x-ptchan-signature must use hmac-sha256"))?;
    let provided = hex::decode(provided).context("x-ptchan-signature is not hex")?;
    let target = uri
        .path_and_query()
        .map_or_else(|| uri.path(), PathAndQuery::as_str);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).context("create hmac")?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(method.as_str().as_bytes());
    mac.update(b".");
    mac.update(target.as_bytes());
    if let Some(body) = body {
        mac.update(b".");
        mac.update(body);
    }
    mac.verify_slice(&provided)
        .context("x-ptchan-signature mismatch")
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, Method, Uri};
    use chrono::{Duration, Utc};
    use hmac::{KeyInit, Mac};

    use super::{verify_request_headers, verify_request_signature, HmacSha256};

    #[test]
    fn posting_signature_covers_body() {
        let body = br#"{"message":"hello"}"#;
        let timestamp = "2026-07-19T12:00:00Z";
        let method = Method::POST;
        let uri = "/integration/v1/threads/i/100/replies"
            .parse::<Uri>()
            .unwrap();
        let signature = signature("secret", timestamp, &method, &uri, Some(body));

        verify_request_signature("secret", timestamp, &method, &uri, Some(body), &signature)
            .unwrap();
        assert!(verify_request_signature(
            "secret",
            timestamp,
            &method,
            &uri,
            Some(br#"{"message":"bye"}"#),
            &signature
        )
        .is_err());
    }

    #[test]
    fn reading_signature_covers_method_path_and_query() {
        let timestamp = "2026-07-19T12:00:00Z";
        let method = Method::GET;
        let uri = "/integration/v1/threads/i/100?limit=25"
            .parse::<Uri>()
            .unwrap();
        let signature = signature("secret", timestamp, &method, &uri, None);

        verify_request_signature("secret", timestamp, &method, &uri, None, &signature).unwrap();
        assert!(verify_request_signature(
            "secret",
            timestamp,
            &method,
            &"/integration/v1/threads/i/100?limit=50".parse().unwrap(),
            None,
            &signature,
        )
        .is_err());
        assert!(verify_request_signature(
            "secret",
            timestamp,
            &Method::POST,
            &uri,
            None,
            &signature,
        )
        .is_err());
    }

    #[test]
    fn request_headers_reject_expired_and_malformed_signatures() {
        let method = Method::GET;
        let uri = "/integration/v1/threads/i/100".parse::<Uri>().unwrap();

        let current = Utc::now().to_rfc3339();
        let mut headers = signed_headers("secret", &current, &method, &uri);
        verify_request_headers("secret", &headers, &method, &uri, None).unwrap();

        let expired = (Utc::now() - Duration::seconds(301)).to_rfc3339();
        headers = signed_headers("secret", &expired, &method, &uri);
        assert!(verify_request_headers("secret", &headers, &method, &uri, None).is_err());

        headers.insert(
            "x-ptchan-signature",
            HeaderValue::from_static("sha256=not-the-contract"),
        );
        assert!(verify_request_headers("secret", &headers, &method, &uri, None).is_err());

        headers.insert(
            "x-ptchan-signature",
            HeaderValue::from_static("hmac-sha256=not-hex"),
        );
        assert!(verify_request_headers("secret", &headers, &method, &uri, None).is_err());
    }

    fn signed_headers(secret: &str, timestamp: &str, method: &Method, uri: &Uri) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-ptchan-timestamp",
            HeaderValue::from_str(timestamp).unwrap(),
        );
        headers.insert(
            "x-ptchan-signature",
            HeaderValue::from_str(&signature(secret, timestamp, method, uri, None)).unwrap(),
        );
        headers
    }

    fn signature(
        secret: &str,
        timestamp: &str,
        method: &Method,
        uri: &Uri,
        body: Option<&[u8]>,
    ) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(method.as_str().as_bytes());
        mac.update(b".");
        mac.update(uri.path_and_query().unwrap().as_str().as_bytes());
        if let Some(body) = body {
            mac.update(b".");
            mac.update(body);
        }
        format!("hmac-sha256={}", hex::encode(mac.finalize().into_bytes()))
    }
}
