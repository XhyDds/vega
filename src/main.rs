use std::time::Instant;
use vega::*;
mod benchmark;

#[tokio::main]
async fn main() -> Result<()> {
    let sc: std::sync::Arc<Context> = Context::new()?;

    tokio::spawn(monitor::metrics::add_metric(sc.clone()));

    let start = Instant::now();
    //benchmark::pi::calc_pi(&sc, Some(1000000), Some(3));
    benchmark::multihead_attention::multihead_attention(&sc, Some(3));
    // benchmark::e::calc_e(&sc, Some(10000), Some(3));
    let end = start.elapsed();
    println!("{:?}", end);

    Ok(())
}
