use std::env;
use std::io;

use colligerenet_sdk::AsyncClient;
use serde_json::{Value, json};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let peer = next_arg(&mut args)?;
    let service = next_arg(&mut args)?;
    let action = next_arg(&mut args)?;
    let payload = args
        .next()
        .map(|raw| serde_json::from_str::<Value>(&raw))
        .transpose()?
        .unwrap_or_else(|| json!({}));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;

    runtime.block_on(async {
        let mut client =
            AsyncClient::connect_default_checked("example.generic.peer-service").await?;
        let result = client
            .request_peer_service::<Value>(peer, service, action, payload)
            .await?;

        println!("{}", serde_json::to_string_pretty(&result)?);

        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>) -> io::Result<String> {
    args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: generic_peer_service <peer-node-id> <service> <action> [json-payload]",
        )
    })
}
