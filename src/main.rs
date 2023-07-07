use std::time::Instant;
use vega::*;
mod benchmark;

#[tokio::main]
async fn main() -> Result<()> {
    let sc: std::sync::Arc<Context> = Context::new()?;

    tokio::spawn(monitor::metrics::add_metric(sc.clone()));

    let start = Instant::now();
    benchmark::pi::calc_pi(&sc, Some(100000), Some(2));
    //benchmark::e::calc_e(&sc, Some(10000000), Some(3));
    let end = start.elapsed();
    println!("{:?}", end);

    Ok(())
}
