use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use reqwest::{Client, StatusCode};
use serde_json::{Map, Value};

use crate::{
    config::{self, PtchanConfig},
    consumer, event,
    session::SessionCookie,
};

pub(crate) const DEFAULT_THREAD_LIMIT: usize = 50;
pub(crate) const MAX_THREAD_LIMIT: usize = 200;

#[derive(Clone)]
pub(crate) struct ThreadReader {
    base_url: String,
    client: Client,
    cookie: Arc<SessionCookie>,
}

impl ThreadReader {
    pub(crate) fn new(cfg: &PtchanConfig, cookie: Arc<SessionCookie>) -> Result<Self> {
        let client = Client::builder()
            .user_agent(cfg.user_agent.clone())
            .build()
            .context("build thread context client")?;
        Ok(Self {
            base_url: cfg.base_url.clone(),
            client,
            cookie,
        })
    }

    pub(crate) async fn fetch_thread(
        &self,
        board: &str,
        thread_id: i64,
        limit: usize,
    ) -> Result<Option<consumer::Thread>> {
        if !config::valid_board_name(board) {
            return Err(anyhow!("invalid board"));
        }
        if thread_id <= 0 {
            return Err(anyhow!("thread_id must be positive"));
        }
        let limit = limit.clamp(1, MAX_THREAD_LIMIT);
        let url = format!(
            "{}/{}/thread/{}.json",
            self.base_url.trim_end_matches('/'),
            board,
            thread_id
        );
        let response = self
            .client
            .get(url)
            .header("accept", "application/json")
            .header("cookie", self.cookie.get())
            .send()
            .await
            .context("fetch ptchan thread")?;
        match response.status() {
            StatusCode::OK => {}
            StatusCode::NOT_FOUND => return Ok(None),
            status => return Err(anyhow!("ptchan thread status {status}")),
        }
        let body = response.text().await.context("read ptchan thread json")?;
        let value = serde_json::from_str::<Value>(&body).context("decode ptchan thread json")?;
        Ok(Some(thread_from_value(
            &self.base_url,
            board,
            thread_id,
            value,
            limit,
        )?))
    }
}

fn thread_from_value(
    base_url: &str,
    board: &str,
    thread_id: i64,
    value: Value,
    limit: usize,
) -> Result<consumer::Thread> {
    let Value::Object(mut root) = value else {
        return Err(anyhow!("thread json must be an object"));
    };
    let replies = root
        .remove("replies")
        .and_then(|value| match value {
            Value::Array(values) => Some(values),
            _ => None,
        })
        .unwrap_or_default();

    let mut posts = Vec::with_capacity(replies.len().saturating_add(1));
    ensure_board(&mut root, board);
    posts.push(event::consumer_post_from_value(
        base_url,
        Value::Object(root),
    )?);
    for reply in replies {
        let Value::Object(mut reply) = reply else {
            continue;
        };
        ensure_board(&mut reply, board);
        ensure_thread(&mut reply, thread_id);
        posts.push(event::consumer_post_from_value(
            base_url,
            Value::Object(reply),
        )?);
    }

    posts.sort_by_key(|post| post.id);
    let truncated = posts.len() > limit;
    if truncated {
        posts = posts.split_off(posts.len() - limit);
    }

    Ok(consumer::Thread {
        board: board.to_string(),
        id: thread_id,
        posts,
        truncated,
    })
}

fn ensure_board(post: &mut Map<String, Value>, board: &str) {
    post.entry("board")
        .or_insert_with(|| Value::String(board.to_string()));
}

fn ensure_thread(post: &mut Map<String, Value>, thread_id: i64) {
    post.entry("thread")
        .or_insert_with(|| Value::Number(thread_id.into()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keeps_recent_posts_in_chronological_order() {
        let thread = thread_from_value(
            "https://ptchan.test",
            "i",
            100,
            json!({
                "date": "2026-07-19T12:00:00.000Z",
                "postId": 100,
                "message": "op",
                "replies": [
                    { "date": "2026-07-19T12:01:00.000Z", "postId": 101, "message": "one" },
                    { "date": "2026-07-19T12:02:00.000Z", "postId": 102, "message": "two" }
                ]
            }),
            2,
        )
        .unwrap();

        assert!(thread.truncated);
        assert_eq!(thread.posts.len(), 2);
        assert_eq!(thread.posts[0].id, 101);
        assert_eq!(thread.posts[1].id, 102);
        assert_eq!(thread.posts[0].thread_id, 100);
    }

    #[test]
    fn rejects_unsafe_board_names() {
        assert!(config::valid_board_name("test"));
        assert!(!config::valid_board_name("../test"));
        assert!(!config::valid_board_name(""));
    }
}
