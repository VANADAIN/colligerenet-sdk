use std::env;

use colligerenet_sdk::AsyncEventStream;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let max_events = env::args()
        .nth(1)
        .map(|raw| raw.parse::<usize>())
        .transpose()?
        .unwrap_or(usize::MAX);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;

    runtime.block_on(async {
        let mut events = AsyncEventStream::connect_default("example.generic.events").await?;

        for _ in 0..max_events {
            let event = events.next_event().await?;
            println!(
                "#{} {} {}: {}",
                event.sequence, event.unix_seconds, event.kind, event.message
            );
        }

        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
