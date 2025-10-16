use clap::Parser;

use dedupe::cli::{run, Cli};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    run(args).await
}
