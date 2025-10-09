use tors_rs::{Client, Config, init_logging};

use anyhow::{Context, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_args().context("Failed to parse configuration")?;
    init_logging(&config);

    let client = Client::new(config)
        .await
        .context("Failed to create Torrent Client.")?;
    client.run().await
}
