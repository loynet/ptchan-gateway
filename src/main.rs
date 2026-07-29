mod config;
mod contract;
mod event;
mod metrics;
mod origin;
mod posting;
mod rate_limit;
mod reading;
mod retention;
mod runtime;
mod session;
mod socket;
mod store;
mod upstream;
mod webhook;

use std::{env, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{anyhow, Context, Result};
use tokio::{
    signal,
    sync::{watch, Notify},
};
use tracing::{error, info, warn};

use crate::{
    config::Config,
    origin::OriginMatcher,
    posting::PostWriter,
    reading::ThreadReader,
    runtime::{HttpServer, Status},
    session::SessionCookie,
    socket::Supervisor,
    store::Store,
};

enum Command {
    Run,
    CheckConfig,
    CheckContract,
    CheckHealth,
    WriteContract,
}

impl FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "--check-config" => Ok(Self::CheckConfig),
            "--check-contract" => Ok(Self::CheckContract),
            "--check-health" => Ok(Self::CheckHealth),
            "--write-contract" => Ok(Self::WriteContract),
            other => Err(anyhow!(
                "unknown argument {other}; use --check-config, --check-contract, --check-health, or --write-contract"
            )),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    match command_from_args()? {
        Command::CheckHealth => {
            let addr =
                env::var("HEALTHCHECK_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
            runtime::check_health(&addr).await
        }
        Command::CheckConfig => {
            let cfg = config::load_from_env().context("load config")?;
            config::init_logging(&cfg.runtime.logging)?;
            println!("configuration ok");
            Ok(())
        }
        Command::CheckContract => {
            contract::artifacts::check(&runtime::contract_openapi())?;
            println!("contract artifacts ok");
            Ok(())
        }
        Command::WriteContract => {
            contract::artifacts::write(&runtime::contract_openapi())?;
            println!(
                "contract artifacts written to {}",
                contract::artifacts::default_dir().display()
            );
            Ok(())
        }
        Command::Run => {
            let cfg = config::load_from_env().context("load config")?;
            config::init_logging(&cfg.runtime.logging)?;
            run(cfg).await
        }
    }
}

fn command_from_args() -> Result<Command> {
    let mut args = env::args().skip(1);
    let command = match args.next() {
        None => Command::Run,
        Some(command) => command.parse()?,
    };
    if args.next().is_some() {
        return Err(anyhow!("too many arguments"));
    }
    Ok(command)
}

async fn run(cfg: Config) -> Result<()> {
    let upstream_required = !cfg.webhooks.is_empty();
    let sqlite_path = PathBuf::from(&cfg.storage.sqlite_path);
    let store = Arc::new(Store::open(&sqlite_path).await.context("open sqlite")?);
    store.migrate().await.context("migrate sqlite")?;
    let thread_reader = ThreadReader::new(&cfg.ptchan).context("create reading client")?;
    let post_writer = PostWriter::new(&cfg.ptchan).context("create posting writer")?;
    let origins = OriginMatcher::new(&cfg.postings);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let status = Arc::new(Status::new(upstream_required));
    let delivery_wakeup = Arc::new(Notify::new());

    let http_handle = runtime::spawn_http(
        HttpServer {
            addr: cfg.runtime.http_addr.clone(),
            status: status.clone(),
            store: store.clone(),
            thread_reader,
            post_writer,
            integrations: cfg.integrations.clone(),
            postings: cfg.postings.clone(),
            origins: origins.clone(),
            rate_limit: cfg.runtime.rate_limit.clone(),
        },
        shutdown_rx.clone(),
    )
    .await
    .context("start runtime http server")?;

    let (refresh_handle, socket_handle) = if upstream_required {
        let cookie = config::ptchan_session_cookie().context("load ptchan session cookie")?;
        let cookie_jar = Arc::new(
            SessionCookie::new(&cookie, &cfg.ptchan.base_url)
                .context("parse management session cookie")?,
        );
        let refresh_handle = tokio::spawn(session::refresh_loop(
            cfg.ptchan.clone(),
            cookie_jar.clone(),
            status.clone(),
            shutdown_rx.clone(),
        ));
        let socket_handle = tokio::spawn(socket::supervise(
            Supervisor {
                cfg: cfg.ptchan.clone(),
                cookie: cookie_jar,
                store: store.clone(),
                webhooks: cfg.webhooks.clone(),
                origins,
                fingerprint_secret: cfg.fingerprint_secret.clone(),
                delivery_wakeup: delivery_wakeup.clone(),
                status: status.clone(),
            },
            shutdown_rx.clone(),
        ));
        (Some(refresh_handle), Some(socket_handle))
    } else {
        info!("no webhook integrations configured; skipping management session and socket");
        (None, None)
    };
    let delivery_handle = tokio::spawn(webhook::delivery_loop(
        cfg.webhooks.clone(),
        store.clone(),
        delivery_wakeup,
        shutdown_rx.clone(),
    ));
    let cleanup_handle = tokio::spawn(retention::cleanup_loop(
        store,
        cfg.storage.event_retention,
        shutdown_rx.clone(),
    ));

    info!("service started");
    wait_for_shutdown().await;
    info!("shutdown requested");
    let _ = shutdown_tx.send(true);

    if let Some(refresh_handle) = refresh_handle {
        if let Err(err) = refresh_handle.await {
            error!(error = %err, "session refresh task failed");
        }
    }
    if let Some(socket_handle) = socket_handle {
        if let Err(err) = socket_handle.await {
            error!(error = %err, "socket supervisor task failed");
        }
    }
    if let Err(err) = delivery_handle.await {
        error!(error = %err, "delivery task failed");
    }
    if let Err(err) = cleanup_handle.await {
        error!(error = %err, "database cleanup task failed");
    }
    http_handle.await??;
    Ok(())
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use signal::unix::{signal as unix_signal, SignalKind};

        match unix_signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    result = signal::ctrl_c() => {
                        if let Err(err) = result {
                            warn!(error = %err, "ctrl-c handler failed");
                        }
                    }
                    _ = term.recv() => {}
                }
            }
            Err(err) => {
                warn!(error = %err, "SIGTERM handler unavailable; waiting for ctrl-c only");
                if let Err(err) = signal::ctrl_c().await {
                    warn!(error = %err, "ctrl-c handler failed");
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(err) = signal::ctrl_c().await {
            warn!(error = %err, "ctrl-c handler failed");
        }
    }
}
