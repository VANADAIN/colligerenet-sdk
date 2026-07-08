use std::env;
use std::fmt;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use colligerenet_api::{
    PeerParams, RemoteServiceRequestParams, RpcError, RpcRequest, RpcResponse, method,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader, ReadHalf, WriteHalf};
use tokio::net::UnixStream as AsyncUnixStream;

pub use colligerenet_api::{
    AdapterStatus, ApiEvent, DaemonHealth, DaemonStatus, DeviceInfo, PeerInfo, ServiceInfo,
};

pub const SUPPORTED_API_VERSION: &str = colligerenet_api::API_VERSION;

pub type SdkResult<T> = Result<T, SdkError>;

#[derive(Debug)]
pub enum SdkError {
    Io(io::Error),
    Json(serde_json::Error),
    Api(RpcError),
    MissingResult { context: String },
    DaemonDisconnected { context: String },
    IncompatibleApiVersion { expected: String, actual: String },
}

impl SdkError {
    pub fn api_code(&self) -> Option<i64> {
        match self {
            Self::Api(error) => Some(error.code),
            _ => None,
        }
    }
}

impl fmt::Display for SdkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "io error: {error}"),
            Self::Json(error) => write!(formatter, "json error: {error}"),
            Self::Api(error) => write!(formatter, "api error {}: {}", error.code, error.message),
            Self::MissingResult { context } => {
                write!(formatter, "api response missing result for {context}")
            }
            Self::DaemonDisconnected { context } => {
                write!(formatter, "daemon disconnected during {context}")
            }
            Self::IncompatibleApiVersion { expected, actual } => write!(
                formatter,
                "incompatible daemon api version: expected {expected}, got {actual}"
            ),
        }
    }
}

impl std::error::Error for SdkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for SdkError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for SdkError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

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

    pub fn connect_default_checked(app_id: impl Into<String>) -> SdkResult<Self> {
        let mut client = Self::connect_default(app_id)?;
        client.check_compatibility()?;

        Ok(client)
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

    pub fn check_compatibility(&mut self) -> SdkResult<DaemonStatus> {
        let status = self.daemon_status()?;
        if status.api_version == SUPPORTED_API_VERSION {
            Ok(status)
        } else {
            Err(SdkError::IncompatibleApiVersion {
                expected: SUPPORTED_API_VERSION.to_owned(),
                actual: status.api_version,
            })
        }
    }

    pub fn daemon_status(&mut self) -> SdkResult<DaemonStatus> {
        self.call(method::DAEMON_STATUS, None)
    }

    pub fn daemon_health(&mut self) -> SdkResult<DaemonHealth> {
        self.call(method::DAEMON_HEALTH, None)
    }

    pub fn adapters(&mut self) -> SdkResult<Vec<AdapterStatus>> {
        self.call(method::ADAPTERS_LIST, None)
    }

    pub fn devices(&mut self) -> SdkResult<Vec<DeviceInfo>> {
        self.call(method::DEVICES_LIST, None)
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
        write_request(&mut self.writer, &request)?;
        let response = read_response(&mut self.reader, method)?;

        decode_response(response, method)
    }
}

#[derive(Debug)]
pub struct AsyncClient {
    app_id: String,
    reader: AsyncBufReader<ReadHalf<AsyncUnixStream>>,
    writer: WriteHalf<AsyncUnixStream>,
    next_id: u64,
}

impl AsyncClient {
    pub async fn connect_default(app_id: impl Into<String>) -> SdkResult<Self> {
        Self::connect(default_socket_path(), app_id).await
    }

    pub async fn connect_default_checked(app_id: impl Into<String>) -> SdkResult<Self> {
        let mut client = Self::connect_default(app_id).await?;
        client.check_compatibility().await?;

        Ok(client)
    }

    pub async fn connect(path: impl AsRef<Path>, app_id: impl Into<String>) -> SdkResult<Self> {
        let stream = AsyncUnixStream::connect(path).await?;
        let (reader, writer) = tokio::io::split(stream);

        Ok(Self {
            app_id: app_id.into(),
            reader: AsyncBufReader::new(reader),
            writer,
            next_id: 1,
        })
    }

    pub async fn check_compatibility(&mut self) -> SdkResult<DaemonStatus> {
        let status = self.daemon_status().await?;
        if status.api_version == SUPPORTED_API_VERSION {
            Ok(status)
        } else {
            Err(SdkError::IncompatibleApiVersion {
                expected: SUPPORTED_API_VERSION.to_owned(),
                actual: status.api_version,
            })
        }
    }

    pub async fn daemon_status(&mut self) -> SdkResult<DaemonStatus> {
        self.call(method::DAEMON_STATUS, None).await
    }

    pub async fn daemon_health(&mut self) -> SdkResult<DaemonHealth> {
        self.call(method::DAEMON_HEALTH, None).await
    }

    pub async fn adapters(&mut self) -> SdkResult<Vec<AdapterStatus>> {
        self.call(method::ADAPTERS_LIST, None).await
    }

    pub async fn devices(&mut self) -> SdkResult<Vec<DeviceInfo>> {
        self.call(method::DEVICES_LIST, None).await
    }

    pub async fn services(&mut self) -> SdkResult<Vec<ServiceInfo>> {
        self.call(method::SERVICES_LIST, None).await
    }

    pub async fn peers(&mut self) -> SdkResult<Vec<PeerInfo>> {
        self.call(method::PEERS_LIST, None).await
    }

    pub async fn peer(&mut self, node_id: impl Into<String>) -> SdkResult<Option<PeerInfo>> {
        self.call(
            method::PEERS_SHOW,
            Some(json!(PeerParams {
                node_id: node_id.into(),
            })),
        )
        .await
    }

    pub async fn remove_peer(&mut self, node_id: impl Into<String>) -> SdkResult<bool> {
        self.call(
            method::PEERS_REMOVE,
            Some(json!(PeerParams {
                node_id: node_id.into(),
            })),
        )
        .await
    }

    pub async fn request_peer_service<T>(
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
        .await
    }

    pub async fn call<T>(&mut self, method: &str, params: Option<Value>) -> SdkResult<T>
    where
        T: DeserializeOwned,
    {
        let id = self.next_id;
        self.next_id += 1;

        let request = RpcRequest::new(method, params, id).with_app_id(self.app_id.clone());
        write_async_request(&mut self.writer, &request).await?;
        let response = read_async_response(&mut self.reader, method).await?;

        decode_response(response, method)
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
            method::SERVICES_SERVE,
            Some(json!({
                "service": service.into(),
                "actions": actions.into_iter().collect::<Vec<_>>()
            })),
            1,
        )
        .with_app_id(app_id.into());

        write_request(&mut writer, &request)?;
        read_success_response(&mut reader, "service host registration")?;

        Ok(Self { reader, writer })
    }

    pub fn next_request(&mut self) -> SdkResult<ServiceHostRequest> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            return Err(SdkError::DaemonDisconnected {
                context: "service host stream".to_owned(),
            });
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
        write_response(&mut self.writer, &response)
    }

    pub fn respond_error(&mut self, code: i64, message: impl Into<String>) -> SdkResult<()> {
        let response = RpcResponse::error(None, code, message);
        write_response(&mut self.writer, &response)
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

        write_request(&mut writer, &request)?;

        Ok(Self {
            reader,
            _writer: writer,
        })
    }

    pub fn next_event(&mut self) -> SdkResult<ApiEvent> {
        let response = read_response(&mut self.reader, method::EVENTS_SUBSCRIBE)?;
        decode_response(response, method::EVENTS_SUBSCRIBE)
    }
}

#[derive(Debug)]
pub struct AsyncEventStream {
    reader: AsyncBufReader<ReadHalf<AsyncUnixStream>>,
    _writer: WriteHalf<AsyncUnixStream>,
}

impl AsyncEventStream {
    pub async fn connect_default(app_id: impl Into<String>) -> SdkResult<Self> {
        Self::connect(default_socket_path(), app_id).await
    }

    pub async fn connect(path: impl AsRef<Path>, app_id: impl Into<String>) -> SdkResult<Self> {
        let stream = AsyncUnixStream::connect(path).await?;
        let (reader, mut writer) = tokio::io::split(stream);
        let request = RpcRequest::new(method::EVENTS_SUBSCRIBE, None, 1).with_app_id(app_id.into());

        write_async_request(&mut writer, &request).await?;

        Ok(Self {
            reader: AsyncBufReader::new(reader),
            _writer: writer,
        })
    }

    pub async fn next_event(&mut self) -> SdkResult<ApiEvent> {
        let response = read_async_response(&mut self.reader, method::EVENTS_SUBSCRIBE).await?;
        decode_response(response, method::EVENTS_SUBSCRIBE)
    }
}

fn write_request(writer: &mut UnixStream, request: &RpcRequest) -> SdkResult<()> {
    serde_json::to_writer(&mut *writer, request)?;
    writer.write_all(b"\n")?;
    writer.flush()?;

    Ok(())
}

fn write_response(writer: &mut UnixStream, response: &RpcResponse) -> SdkResult<()> {
    serde_json::to_writer(&mut *writer, response)?;
    writer.write_all(b"\n")?;
    writer.flush()?;

    Ok(())
}

async fn write_async_request(
    writer: &mut WriteHalf<AsyncUnixStream>,
    request: &RpcRequest,
) -> SdkResult<()> {
    let mut line = serde_json::to_vec(request)?;
    line.push(b'\n');
    writer.write_all(&line).await?;
    writer.flush().await?;

    Ok(())
}

fn read_response(reader: &mut BufReader<UnixStream>, context: &str) -> SdkResult<RpcResponse> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if line.trim().is_empty() {
        return Err(SdkError::DaemonDisconnected {
            context: context.to_owned(),
        });
    }

    Ok(serde_json::from_str(&line)?)
}

async fn read_async_response(
    reader: &mut AsyncBufReader<ReadHalf<AsyncUnixStream>>,
    context: &str,
) -> SdkResult<RpcResponse> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    if line.trim().is_empty() {
        return Err(SdkError::DaemonDisconnected {
            context: context.to_owned(),
        });
    }

    Ok(serde_json::from_str(&line)?)
}

fn read_success_response(reader: &mut BufReader<UnixStream>, context: &str) -> SdkResult<Value> {
    let response = read_response(reader, context)?;
    decode_response(response, context)
}

fn decode_response<T>(response: RpcResponse, context: &str) -> SdkResult<T>
where
    T: DeserializeOwned,
{
    let result = decode_value_response(response, context)?;

    Ok(serde_json::from_value(result)?)
}

fn decode_value_response(response: RpcResponse, context: &str) -> SdkResult<Value> {
    if let Some(error) = response.error {
        return Err(SdkError::Api(error));
    }

    response.result.ok_or_else(|| SdkError::MissingResult {
        context: context.to_owned(),
    })
}

pub fn default_socket_path() -> PathBuf {
    env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir)
        .join("colligerenet")
        .join("daemon.sock")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::net::UnixListener;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use colligerenet_api::error_code;

    #[test]
    fn exposes_api_error_code() {
        let response = RpcResponse::error(Some(Value::from(1)), error_code::UNAUTHORIZED, "denied");
        let error = decode_response::<Value>(response, "test").expect_err("response should fail");

        assert_eq!(error.api_code(), Some(error_code::UNAUTHORIZED));
    }

    #[test]
    fn event_stream_reports_daemon_disconnect() {
        let path = temp_socket_path("event-disconnect");
        let listener = UnixListener::bind(&path).expect("listener should bind");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("client should connect");
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .expect("subscribe request should read");
        });

        let mut stream = EventStream::connect(&path, "test.app").expect("stream should connect");
        let error = stream.next_event().expect_err("disconnect should fail");

        assert!(matches!(error, SdkError::DaemonDisconnected { .. }));
        server.join().expect("server should finish");
        fs::remove_file(path).expect("socket should be removed");
    }

    #[test]
    fn async_event_stream_reports_daemon_disconnect() {
        let path = temp_socket_path("async-event-disconnect");
        let listener = UnixListener::bind(&path).expect("listener should bind");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("client should connect");
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .expect("subscribe request should read");
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("runtime should build");
        runtime.block_on(async {
            let mut stream = AsyncEventStream::connect(&path, "test.app")
                .await
                .expect("stream should connect");
            let error = stream
                .next_event()
                .await
                .expect_err("disconnect should fail");

            assert!(matches!(error, SdkError::DaemonDisconnected { .. }));
        });

        server.join().expect("server should finish");
        fs::remove_file(path).expect("socket should be removed");
    }

    fn temp_socket_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        env::temp_dir().join(format!(
            "colligerenet-sdk-{name}-{}-{unique}.sock",
            std::process::id()
        ))
    }
}
