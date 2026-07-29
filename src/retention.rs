use std::{sync::Arc, time::Duration};

use chrono::{Duration as ChronoDuration, Utc};
use tokio::{sync::watch, time};
use tracing::{debug, info, warn};

use crate::store::Store;

const CLEANUP_INTERVAL: Duration = Duration::from_hours(1);

pub(crate) async fn cleanup_loop(
    store: Arc<Store>,
    retention: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        prune_once(&store, retention).await;
        tokio::select! {
            _ = shutdown.changed() => {}
            () = time::sleep(CLEANUP_INTERVAL) => {}
        }
    }
}

async fn prune_once(store: &Store, retention: Duration) {
    let Ok(retention) = ChronoDuration::from_std(retention) else {
        warn!("storage event retention is too large to convert");
        return;
    };
    let cutoff = Utc::now() - retention;
    match store.prune_delivered_events(cutoff).await {
        Ok(0) => debug!(%cutoff, "database retention cleanup found no delivered events"),
        Ok(deleted) => {
            info!(%cutoff, deleted, "database retention cleanup pruned delivered events");
        }
        Err(err) => warn!(error = %err, "database retention cleanup failed"),
    }
}
