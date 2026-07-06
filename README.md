# ColligereNet SDK

Rust client SDK for the local ColligereNet daemon API.

The first implementation talks to the daemon over a Unix domain socket using
JSON-RPC 2.0. Apps identify themselves with an app id that the local daemon can
authorize against a manifest grant.

## Example

Start the daemon API server from the `colligere` repository:

```sh
make daemon-api
```

Then call it from a Rust app:

```rust
use colligerenet_sdk::Client;

let mut client = Client::connect_default("my.app")?;
let status = client.daemon_status()?;
println!("{} {}", status.node_id, status.version);
# Ok::<(), Box<dyn std::error::Error>>(())
```
