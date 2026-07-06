use std::env;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use colligerenet_api::{
    AdapterStatus, ClipboardItem, ClipboardPublishParams, DaemonStatus, DatetimeStatus, RpcRequest,
    RpcResponse, ServiceInfo, error_code, method,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

pub type SdkResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
pub struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    next_id: u64,
}

impl Client {
    pub fn connect_default() -> SdkResult<Self> {
        Self::connect(default_socket_path())
    }

    pub fn connect(path: impl AsRef<Path>) -> SdkResult<Self> {
        let stream = UnixStream::connect(path)?;
        let reader = BufReader::new(stream.try_clone()?);

        Ok(Self {
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

    pub fn datetime_status(&mut self) -> SdkResult<DatetimeStatus> {
        self.call(method::DATETIME_STATUS, None)
    }

    pub fn clipboard_publish(&mut self, content: impl Into<String>) -> SdkResult<ClipboardItem> {
        self.call(
            method::CLIPBOARD_PUBLISH,
            Some(json!(ClipboardPublishParams {
                content: content.into(),
            })),
        )
    }

    pub fn clipboard_get(&mut self) -> SdkResult<Option<ClipboardItem>> {
        self.call(method::CLIPBOARD_GET, None)
    }

    pub fn call<T>(&mut self, method: &str, params: Option<Value>) -> SdkResult<T>
    where
        T: DeserializeOwned,
    {
        let id = self.next_id;
        self.next_id += 1;

        let request = RpcRequest::new(method, params, id);
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

pub fn default_socket_path() -> PathBuf {
    env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir)
        .join("colligerenet")
        .join("daemon.sock")
}
