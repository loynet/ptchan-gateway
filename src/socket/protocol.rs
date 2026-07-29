use anyhow::{Context, Result};
use serde_json::Value;

pub(super) enum Packet {
    EngineOpen,
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
        "0" => Ok(Packet::EngineOpen),
        "1" => Ok(Packet::EngineClose),
        "2" => Ok(Packet::EnginePing),
        "4" => decode_socketio(payload),
        _ => Ok(Packet::Ignored {
            layer: "engine.io",
            code: code.to_string(),
        }),
    }
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
        assert!(matches!(decode("0{}").unwrap(), Packet::EngineOpen));
        assert!(matches!(decode("2").unwrap(), Packet::EnginePing));
        assert!(matches!(decode("40{}").unwrap(), Packet::SocketConnected));
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
