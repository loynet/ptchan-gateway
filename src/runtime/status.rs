use std::sync::atomic::{AtomicBool, Ordering};

use crate::metrics;

pub(crate) struct Status {
    upstream_joined: AtomicBool,
    auth_healthy: AtomicBool,
    upstream_required: AtomicBool,
}

impl Default for Status {
    fn default() -> Self {
        Self::new(true)
    }
}

impl Status {
    pub(crate) fn new(upstream_required: bool) -> Self {
        metrics::UPSTREAM_REQUIRED.set(i64::from(upstream_required));
        Self {
            upstream_joined: AtomicBool::new(false),
            auth_healthy: AtomicBool::new(false),
            upstream_required: AtomicBool::new(upstream_required),
        }
    }

    pub(crate) fn set_upstream_joined(&self, joined: bool) {
        self.upstream_joined.store(joined, Ordering::Relaxed);
        metrics::SOCKET_JOINED.set(i64::from(joined));
    }

    pub(crate) fn set_auth_healthy(&self, healthy: bool) {
        self.auth_healthy.store(healthy, Ordering::Relaxed);
        metrics::UPSTREAM_AUTH_HEALTHY.set(i64::from(healthy));
    }

    pub(crate) fn auth_healthy(&self) -> bool {
        self.auth_healthy.load(Ordering::Relaxed)
    }

    pub(crate) fn upstream_joined(&self) -> bool {
        self.upstream_joined.load(Ordering::Relaxed)
    }

    pub(crate) fn upstream_required(&self) -> bool {
        self.upstream_required.load(Ordering::Relaxed)
    }

    pub(crate) fn ready(&self) -> bool {
        !self.upstream_required() || (self.auth_healthy() && self.upstream_joined())
    }
}
