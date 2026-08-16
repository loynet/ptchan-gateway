use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::Value;

#[derive(Clone, Copy, Debug)]
pub(super) struct Heartbeat {
    pub(super) ping_interval: Duration,
    pub(super) ping_timeout: Duration,
}

pub(super) enum Packet {
    EngineOpen { heartbeat: Heartbeat },
    EnginePing,
    EngineClose,
    SocketConnected,
    SocketDisconnected { payload_bytes: usize },
    SocketEvent { name: String, payload: Vec<Value> },
    SocketError { payload_bytes: usize },
    Ignored { layer: &'static str, code: String },
}

pub(super) fn decode(text: &str) -> Result<Packet> {
    let (code, payload) = text
        .split_at_checked(1)
        .context("socket packet was empty or did not start with an ASCII packet code")?;
    match code {
        "0" => decode_engine_open(payload),
        "1" => Ok(Packet::EngineClose),
        "2" => Ok(Packet::EnginePing),
        "4" => decode_socketio(payload),
        _ => Ok(Packet::Ignored {
            layer: "engine.io",
            code: code.to_string(),
        }),
    }
}

fn decode_engine_open(payload: &str) -> Result<Packet> {
    let value =
        serde_json::from_str::<Value>(payload).context("engine.io open payload was not JSON")?;
    let object = value
        .as_object()
        .context("engine.io open payload was not an object")?;
    let ping_interval = duration_millis(object, "pingInterval")?;
    let ping_timeout = duration_millis(object, "pingTimeout")?;
    Ok(Packet::EngineOpen {
        heartbeat: Heartbeat {
            ping_interval,
            ping_timeout,
        },
    })
}

fn duration_millis(object: &serde_json::Map<String, Value>, name: &str) -> Result<Duration> {
    let millis = object
        .get(name)
        .and_then(Value::as_u64)
        .filter(|millis| *millis > 0)
        .with_context(|| format!("engine.io open payload did not include a positive {name}"))?;
    Ok(Duration::from_millis(millis))
}

fn decode_socketio(text: &str) -> Result<Packet> {
    let (code, payload) = text
        .split_at_checked(1)
        .context("socket.io packet was empty or did not start with an ASCII packet code")?;
    match code {
        "0" => Ok(Packet::SocketConnected),
        "1" => Ok(Packet::SocketDisconnected {
            payload_bytes: payload.len(),
        }),
        "2" => decode_event(payload),
        "4" => Ok(Packet::SocketError {
            payload_bytes: payload.len(),
        }),
        _ => Ok(Packet::Ignored {
            layer: "socket.io",
            code: code.to_string(),
        }),
    }
}

fn decode_event(payload: &str) -> Result<Packet> {
    let mut values =
        serde_json::from_str::<Vec<Value>>(payload).context("socket event payload was not JSON")?;
    if values.is_empty() {
        anyhow::bail!("socket event did not include a name");
    }
    let name = values
        .remove(0)
        .as_str()
        .context("socket event name was not text")?
        .to_string();
    Ok(Packet::SocketEvent {
        name,
        payload: values,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn decodes_engine_and_socket_packets() {
        let Packet::EngineOpen { heartbeat } =
            decode(r#"0{"pingInterval":25000,"pingTimeout":20000}"#).unwrap()
        else {
            panic!("expected engine open");
        };
        assert_eq!(heartbeat.ping_interval, Duration::from_secs(25));
        assert_eq!(heartbeat.ping_timeout, Duration::from_secs(20));
        assert!(matches!(decode("2").unwrap(), Packet::EnginePing));
        assert!(matches!(decode("40{}").unwrap(), Packet::SocketConnected));
    }

    #[test]
    fn rejects_engine_open_without_heartbeat_settings() {
        assert!(decode("0{}").is_err());
        assert!(decode(r#"0{"pingInterval":0,"pingTimeout":20000}"#).is_err());
    }

    #[test]
    fn decodes_socket_event_name_and_payload() {
        let Packet::SocketEvent { name, payload } =
            decode(r#"42["newPost",{"postId":123}]"#).unwrap()
        else {
            panic!("expected socket event");
        };

        assert_eq!(name, "newPost");
        assert_eq!(payload, vec![json!({"postId": 123})]);
    }

    #[test]
    fn rejects_malformed_socket_events() {
        assert!(decode("").is_err());
        assert!(decode("42not-json").is_err());
        assert!(decode(r#"42[{"postId":123}]"#).is_err());
    }
}
