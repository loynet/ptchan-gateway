use std::{collections::HashMap, sync::Arc};

use crate::{
    config::PostingConfig,
    contract::{OriginKind, Post, PostOrigin, Thread},
};

#[derive(Clone)]
pub(crate) struct OriginMatcher {
    integrations: Arc<HashMap<String, String>>,
}

impl OriginMatcher {
    pub(crate) fn new(postings: &[PostingConfig]) -> Self {
        Self {
            integrations: Arc::new(
                postings
                    .iter()
                    .map(|posting| (posting.public_tripcode.clone(), posting.name.clone()))
                    .collect(),
            ),
        }
    }

    pub(crate) fn annotate_post(&self, post: &mut Post) {
        post.origin = post
            .tripcode
            .as_deref()
            .and_then(|tripcode| self.integrations.get(tripcode))
            .map(|name| PostOrigin {
                kind: OriginKind::Integration,
                name: name.clone(),
            });
    }

    pub(crate) fn annotate_thread(&self, thread: &mut Thread) {
        for post in &mut thread.posts {
            self.annotate_post(post);
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    #[test]
    fn identifies_configured_public_tripcode() {
        let matcher = OriginMatcher::new(&[posting("assistant", "!!known")]);
        let mut post = post(Some("!!known"));

        matcher.annotate_post(&mut post);

        assert_eq!(post.origin.unwrap().name, "assistant");
    }

    #[test]
    fn leaves_unknown_tripcode_unattributed() {
        let matcher = OriginMatcher::new(&[posting("assistant", "!!known")]);
        let mut post = post(Some("!!someone-else"));

        matcher.annotate_post(&mut post);

        assert!(post.origin.is_none());
    }

    #[test]
    fn annotates_read_contracts_with_public_integration_identity() {
        let matcher = OriginMatcher::new(&[posting("assistant", "!!X8NXmAS44=")]);
        let mut thread = Thread {
            board: "i".to_string(),
            id: 100,
            posts: vec![post(Some("!!X8NXmAS44="))],
            truncated: false,
        };

        matcher.annotate_thread(&mut thread);

        assert_eq!(thread.posts[0].tripcode.as_deref(), Some("!!X8NXmAS44="));
        assert_eq!(thread.posts[0].origin.as_ref().unwrap().name, "assistant");
        let payload = serde_json::to_string(&thread).unwrap();
        assert!(payload.contains(r#""tripcode":"!!X8NXmAS44=""#));
        assert!(payload.contains(r#""origin":{"kind":"integration","name":"assistant"}"#));
        assert!(!payload.contains("tripcode-secret"));
    }

    fn posting(name: &str, public_tripcode: &str) -> PostingConfig {
        PostingConfig {
            name: name.to_string(),
            allowed_boards: Vec::new(),
            display_name: None,
            secret: "integration-secret".to_string(),
            tripcode_secret: "tripcode-secret".to_string(),
            public_tripcode: public_tripcode.to_string(),
            post_password: "post-secret".to_string(),
        }
    }

    fn post(tripcode: Option<&str>) -> Post {
        Post {
            board: "i".to_string(),
            thread_id: 100,
            id: 101,
            url: "https://ptchan.test/i/thread/100.html#101".to_string(),
            date: Utc::now(),
            subject: None,
            message: Some("body".to_string()),
            name: None,
            tripcode: tripcode.map(str::to_string),
            capcode: None,
            donor: None,
            country: None,
            poster_fingerprint: None,
            origin: None,
            attachment_count: 0,
            references: Vec::new(),
            referenced_by: Vec::new(),
        }
    }
}
