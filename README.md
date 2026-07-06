# ColligereNet SDK

Rust client SDK for the local ColligereNet daemon API.

The first implementation talks to the daemon over a Unix domain socket using
JSON-RPC 2.0. During early development this crate depends on the local
`../colligere/crates/colligerenet-api` checkout.

## Example

Start the daemon API server from the `colligere` repository:

```sh
make daemon-api
```

Then call it from a Rust app:

```rust
use colligerenet_sdk::Client;

let mut client = Client::connect_default()?;
let status = client.daemon_status()?;
println!("{} {}", status.node_id, status.version);
# Ok::<(), Box<dyn std::error::Error>>(())
```
