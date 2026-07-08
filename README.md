# ColligereNet SDK

Rust client SDK for the local ColligereNet daemon API.

The SDK talks to the local daemon over a Unix domain socket using line-delimited
JSON-RPC 2.0. It stays generic: app-specific DTOs, service names, and workflows
belong in application repositories.

`AsyncClient` and `AsyncEventStream` are the primary API direction for new apps.
The blocking `Client` and `EventStream` types remain available for simple tools
and compatibility wrappers.

## App Identity And Grants

Every SDK connection sends an app id. The daemon authorizes that app id against
its local app manifest grants before serving methods or service/action requests.

Minimal manifest shape:

```json
{
  "app_id": "my.app",
  "name": "My App",
  "permissions": [
    { "method": "daemon.status" },
    { "method": "remote.services.request", "service": "owner.service.v1", "actions": ["status"] }
  ]
}
```

Authorization failures are returned as `SdkError::Api`. Use
`error.api_code() == Some(colligerenet_api::error_code::UNAUTHORIZED)` when an
app needs to distinguish missing grants from other daemon errors.

## Compatibility

`SUPPORTED_API_VERSION` is the daemon API version this SDK was built against.
Use `connect_default_checked` to fail early when the local daemon reports a
different API version.

SDK releases should be tagged independently when the SDK public API or daemon
API compatibility changes. Patch releases can stay compatible with the same
daemon API version; breaking daemon protocol changes require a new SDK release
and compatibility notes.

## Examples

Start the daemon API server from the `colligere` repository:

```sh
make daemon-api
```

Then call it from a Rust app:

```rust
use colligerenet_sdk::AsyncClient;

let mut client = AsyncClient::connect_default_checked("my.app").await?;
let status = client.daemon_status().await?;
println!("{} {}", status.node_id, status.version);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Peer apps should use generic service requests and keep app-specific behavior in
the app layer:

```rust
use colligerenet_sdk::AsyncClient;
use serde_json::{Value, json};

let mut client = AsyncClient::connect_default_checked("my.app").await?;
let result = client.request_peer_service::<Value>(
    "<peer-node-id>",
    "example.service.v1",
    "status",
    json!({}),
).await?;
println!("{result}");
# Ok::<(), Box<dyn std::error::Error>>(())
```

Runnable examples:

```sh
cargo run --example generic_peer_service -- <peer-node-id> example.service.v1 status '{}'
cargo run --example event_stream -- 10
```
