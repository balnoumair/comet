use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::{ClientFrame, RpcError, ServerFrame};

const STREAM_QUEUE_CAP: usize = 256;

enum Pending {
    Call(oneshot::Sender<Result<serde_json::Value, RpcError>>),
    Stream(mpsc::Sender<serde_json::Value>),
}

struct Shared {
    pending: Mutex<HashMap<u64, Pending>>,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Pending>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub struct RpcClient {
    out: mpsc::Sender<String>,
    shared: Arc<Shared>,
    next_id: AtomicU64,
    reader: tokio::task::JoinHandle<()>,
}

impl RpcClient {
    pub fn new(out: mpsc::Sender<String>, mut inbound: mpsc::Receiver<String>) -> Self {
        let shared = Arc::new(Shared {
            pending: Mutex::new(HashMap::new()),
        });
        let reader_shared = shared.clone();
        let reader_out = out.clone();
        let reader = tokio::spawn(async move {
            while let Some(payload) = inbound.recv().await {
                for line in payload.lines() {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    let frame: ServerFrame = match serde_json::from_str(line) {
                        Ok(frame) => frame,
                        Err(err) => {
                            tracing::warn!(error = %err, "rpc: dropping malformed server frame");
                            continue;
                        }
                    };
                    route_frame(&reader_shared, &reader_out, frame).await;
                }
            }
            let drained: Vec<Pending> = {
                let mut pending = reader_shared.lock();
                pending.drain().map(|(_, p)| p).collect()
            };
            for entry in drained {
                if let Pending::Call(tx) = entry {
                    let _ = tx.send(Err(RpcError::Closed));
                }
            }
        });
        Self {
            out,
            shared,
            next_id: AtomicU64::new(1),
            reader,
        }
    }

    pub async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.shared.lock().insert(id, Pending::Call(tx));
        self.send(ClientFrame {
            id,
            method: Some(method.into()),
            params,
            cancel: false,
        })
        .await
        .inspect_err(|_| {
            self.shared.lock().remove(&id);
        })?;
        rx.await.map_err(|_| RpcError::Closed)?
    }

    pub async fn call_as<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, RpcError> {
        let value = self.call(method, params).await?;
        serde_json::from_value(value).map_err(|e| RpcError::BadParams(e.to_string()))
    }

    pub async fn subscribe(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<mpsc::Receiver<serde_json::Value>, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(STREAM_QUEUE_CAP);
        self.shared.lock().insert(id, Pending::Stream(tx));
        self.send(ClientFrame {
            id,
            method: Some(method.into()),
            params,
            cancel: false,
        })
        .await
        .inspect_err(|_| {
            self.shared.lock().remove(&id);
        })?;
        Ok(rx)
    }

    async fn send(&self, frame: ClientFrame) -> Result<(), RpcError> {
        let json = serde_json::to_string(&frame)
            .map_err(|e| RpcError::Transport(format!("serialize frame: {e}")))?;
        self.out.send(json).await.map_err(|_| RpcError::Closed)
    }
}

impl Drop for RpcClient {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

async fn route_frame(shared: &Arc<Shared>, out: &mpsc::Sender<String>, frame: ServerFrame) {
    let id = frame.id;
    if let Some(err) = frame.err {
        match shared.lock().remove(&id) {
            Some(Pending::Call(tx)) => {
                let _ = tx.send(Err(RpcError::Failed(err)));
            }
            Some(Pending::Stream(_)) | None => {
                tracing::debug!(id, %err, "rpc: stream ended with error");
            }
        }
        return;
    }
    if let Some(value) = frame.ok {
        if let Some(Pending::Call(tx)) = shared.lock().remove(&id) {
            let _ = tx.send(Ok(value));
        }
        return;
    }
    if let Some(item) = frame.item {
        let tx = match shared.lock().get(&id) {
            Some(Pending::Stream(tx)) => Some(tx.clone()),
            _ => None,
        };
        let dead = match tx {
            Some(tx) => tx.send(item).await.is_err(),
            None => false,
        };
        if dead {
            shared.lock().remove(&id);
            if let Ok(json) = serde_json::to_string(&ClientFrame {
                id,
                method: None,
                params: serde_json::Value::Null,
                cancel: true,
            }) {
                let _ = out.send(json).await;
            }
        }
        return;
    }
    if frame.done {
        shared.lock().remove(&id);
    }
}

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub async fn connect_ws(url: &str) -> Result<RpcClient, RpcError> {
    let (ws, _) = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url))
        .await
        .map_err(|_| RpcError::Transport(format!("timed out dialing {url}")))?
        .map_err(|e| RpcError::Transport(e.to_string()))?;
    let (mut sink, mut stream) = ws.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);
    let (in_tx, in_rx) = mpsc::channel::<String>(256);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                frame = out_rx.recv() => match frame {
                    Some(text) => {
                        if sink.send(WsMessage::Text(text)).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        let _ = sink.send(WsMessage::Close(None)).await;
                        break;
                    }
                },
                message = stream.next() => match message {
                    Some(Ok(WsMessage::Text(text))) => {
                        if in_tx.send(text).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                },
            }
        }
    });
    Ok(RpcClient::new(out_tx, in_rx))
}
