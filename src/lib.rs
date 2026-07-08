use std::env;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use colligerenet_api::{
    PeerParams, RemoteServiceRequestParams, RpcRequest, RpcResponse, error_code, method,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub use colligerenet_api::{AdapterStatus, ApiEvent, DaemonStatus, PeerInfo, ServiceInfo};

pub type SdkResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SERVICES_SERVE_METHOD: &str = "services.serve";

#[derive(Debug)]
pub struct Client {
    app_id: String,
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    next_id: u64,
}

impl Client {
    pub fn connect_default(app_id: impl Into<String>) -> SdkResult<Self> {
        Self::connect(default_socket_path(), app_id)
    }

    pub fn connect(path: impl AsRef<Path>, app_id: impl Into<String>) -> SdkResult<Self> {
        let stream = UnixStream::connect(path)?;
        let reader = BufReader::new(stream.try_clone()?);

        Ok(Self {
            app_id: app_id.into(),
            reader,
            writer: stream,
            next_id: 1,
        })
    }

    pub fn daemon_status(&mut self) -> SdkResult<DaemonStatus> {
        self.call(method::DAEMON_STATUS, None)
    }

    pub fn adapters(&mut self) -> SdkResult<Vec<AdapterStatus>> {
        self.call(method::ADAPTERS_LIST, None)
    }

    pub fn services(&mut self) -> SdkResult<Vec<ServiceInfo>> {
        self.call(method::SERVICES_LIST, None)
    }

    pub fn peers(&mut self) -> SdkResult<Vec<PeerInfo>> {
        self.call(method::PEERS_LIST, None)
    }

    pub fn peer(&mut self, node_id: impl Into<String>) -> SdkResult<Option<PeerInfo>> {
        self.call(
            method::PEERS_SHOW,
            Some(json!(PeerParams {
                node_id: node_id.into(),
            })),
        )
    }

    pub fn remove_peer(&mut self, node_id: impl Into<String>) -> SdkResult<bool> {
        self.call(
            method::PEERS_REMOVE,
            Some(json!(PeerParams {
                node_id: node_id.into(),
            })),
        )
    }

    pub fn request_peer_service<T>(
        &mut self,
        node_id: impl Into<String>,
        service: impl Into<String>,
        action: impl Into<String>,
        payload: Value,
    ) -> SdkResult<T>
    where
        T: DeserializeOwned,
    {
        self.call(
            method::REMOTE_SERVICES_REQUEST,
            Some(json!(RemoteServiceRequestParams {
                node_id: node_id.into(),
                service: service.into(),
                action: action.into(),
                payload,
            })),
        )
    }

    pub fn call<T>(&mut self, method: &str, params: Option<Value>) -> SdkResult<T>
    where
        T: DeserializeOwned,
    {
        let id = self.next_id;
        self.next_id += 1;

        let request = RpcRequest::new(method, params, id).with_app_id(self.app_id.clone());
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;

        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            return Err("daemon returned an empty response".into());
        }

        let response = serde_json::from_str::<RpcResponse>(&line)?;
        if let Some(error) = response.error {
            return Err(format!("api error {}: {}", error.code, error.message).into());
        }

        let result = response.result.ok_or_else(|| {
            format!(
                "api error {}: response missing result",
                error_code::INTERNAL_ERROR
            )
        })?;

        Ok(serde_json::from_value(result)?)
    }
}

#[derive(Debug)]
pub struct ServiceHost {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceHostAction {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ServiceHostRequest {
    pub service: String,
    pub action: String,
    pub payload: Value,
}

impl ServiceHost {
    pub fn serve_default(
        app_id: impl Into<String>,
        service: impl Into<String>,
        actions: impl IntoIterator<Item = ServiceHostAction>,
    ) -> SdkResult<Self> {
        Self::serve(default_socket_path(), app_id, service, actions)
    }

    pub fn serve(
        path: impl AsRef<Path>,
        app_id: impl Into<String>,
        service: impl Into<String>,
        actions: impl IntoIterator<Item = ServiceHostAction>,
    ) -> SdkResult<Self> {
        let mut writer = UnixStream::connect(path)?;
        let mut reader = BufReader::new(writer.try_clone()?);
        let request = RpcRequest::new(
            SERVICES_SERVE_METHOD,
            Some(json!({
                "service": service.into(),
                "actions": actions.into_iter().collect::<Vec<_>>()
            })),
            1,
        )
        .with_app_id(app_id.into());

        serde_json::to_writer(&mut writer, &request)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        ensure_success_response(&mut reader, "service host registration")?;

        Ok(Self { reader, writer })
    }

    pub fn next_request(&mut self) -> SdkResult<ServiceHostRequest> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            return Err("daemon service host stream ended".into());
        }

        let request = serde_json::from_str::<RpcRequest>(&line)?;
        Ok(serde_json::from_value(
            request.params.unwrap_or(Value::Null),
        )?)
    }

    pub fn respond<T>(&mut self, result: T) -> SdkResult<()>
    where
        T: Serialize,
    {
        let response = RpcResponse::success(None, serde_json::to_value(result)?);
        serde_json::to_writer(&mut self.writer, &response)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;

        Ok(())
    }

    pub fn respond_error(&mut self, code: i64, message: impl Into<String>) -> SdkResult<()> {
        let response = RpcResponse::error(None, code, message);
        serde_json::to_writer(&mut self.writer, &response)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct EventStream {
    reader: BufReader<UnixStream>,
    _writer: UnixStream,
}

impl EventStream {
    pub fn connect_default(app_id: impl Into<String>) -> SdkResult<Self> {
        Self::connect(default_socket_path(), app_id)
    }

    pub fn connect(path: impl AsRef<Path>, app_id: impl Into<String>) -> SdkResult<Self> {
        let mut writer = UnixStream::connect(path)?;
        let reader = BufReader::new(writer.try_clone()?);
        let request = RpcRequest::new(method::EVENTS_SUBSCRIBE, None, 1).with_app_id(app_id.into());

        serde_json::to_writer(&mut writer, &request)?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        Ok(Self {
            reader,
            _writer: writer,
        })
    }

    pub fn next_event(&mut self) -> SdkResult<ApiEvent> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            return Err("daemon event stream ended".into());
        }

        let response = serde_json::from_str::<RpcResponse>(&line)?;
        if let Some(error) = response.error {
            return Err(format!("api error {}: {}", error.code, error.message).into());
        }

        let result = response.result.ok_or_else(|| {
            format!(
                "api error {}: event response missing result",
                error_code::INTERNAL_ERROR
            )
        })?;

        Ok(serde_json::from_value(result)?)
    }
}

fn ensure_success_response(reader: &mut BufReader<UnixStream>, context: &str) -> SdkResult<Value> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if line.trim().is_empty() {
        return Err(format!("daemon returned an empty response for {context}").into());
    }

    let response = serde_json::from_str::<RpcResponse>(&line)?;
    if let Some(error) = response.error {
        return Err(format!("api error {}: {}", error.code, error.message).into());
    }

    response.result.ok_or_else(|| {
        format!(
            "api error {}: response missing result for {context}",
            error_code::INTERNAL_ERROR
        )
        .into()
    })
}

pub fn default_socket_path() -> PathBuf {
    env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir)
        .join("colligerenet")
        .join("daemon.sock")
}
