// Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
// Ported from actix-web (actix actor model) to Aethonan's native websockets
// handles (Aethonan: Fidus Writer fork of Granian).

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use tokio::sync::mpsc;
use uuid::Uuid;

/// Commands the Python-visible `WebSocketConnector` sends towards a connected
/// client; consumed by the connection's session task, which owns the sink half
/// of the websocket stream (the actix `Handler<...>` impls in stock Robyn).
#[derive(Clone, Debug)]
pub enum WsCommand {
    SendText(String),
    SendBinary(Vec<u8>),
}

/// Per-endpoint registry of connected clients: client id → command channel.
/// Replaces the actix `WebSocketRegistry` actor.
#[derive(Default)]
pub struct WebSocketRegistry {
    pub clients: DashMap<Uuid, mpsc::UnboundedSender<WsCommand>>,
}

static REGISTRIES: OnceLock<RwLock<HashMap<String, Arc<WebSocketRegistry>>>> = OnceLock::new();

pub fn registry_for(endpoint: &str) -> Arc<WebSocketRegistry> {
    let map = REGISTRIES.get_or_init(|| RwLock::new(HashMap::new()));
    {
        let map = map.read().unwrap();
        if let Some(registry) = map.get(endpoint) {
            return Arc::clone(registry);
        }
    }
    let registry = Arc::new(WebSocketRegistry::default());
    map.write()
        .unwrap()
        .insert(endpoint.to_string(), Arc::clone(&registry));
    registry
}

impl WebSocketRegistry {
    pub fn send_text_to(&self, recipient_id: Uuid, message: String) {
        if let Some(sender) = self.clients.get(&recipient_id) {
            let _ = sender.send(WsCommand::SendText(message));
        } else {
            log::warn!("No client found for id: {}", recipient_id);
        }
    }

    pub fn send_binary_to(&self, recipient_id: Uuid, data: Vec<u8>) {
        if let Some(sender) = self.clients.get(&recipient_id) {
            let _ = sender.send(WsCommand::SendBinary(data));
        } else {
            log::warn!("No client found for id: {}", recipient_id);
        }
    }

    pub fn broadcast_text(&self, message: String) {
        for entry in self.clients.iter() {
            let _ = entry.value().send(WsCommand::SendText(message.clone()));
        }
    }

    pub fn broadcast_binary(&self, data: Vec<u8>) {
        for entry in self.clients.iter() {
            let _ = entry.value().send(WsCommand::SendBinary(data.clone()));
        }
    }
}
