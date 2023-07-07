use std::time::Instant;
use vega::*;
mod benchmark;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = monitor::poster::post(String::from("preparing")).await;
    let sc: std::sync::Arc<Context> = Context::new()?;

    tokio::spawn(monitor::metrics::add_metric(sc.clone()));

    let _ = monitor::poster::post(String::from("running")).await;

    let start = Instant::now();
    benchmark::pi::calc_pi(&sc, Some(1000000), Some(2));
    // benchmark::multihead_attention::multihead_attention(&sc, Some(10000), Some(100), Some(3));
    // benchmark::e::calc_e(&sc, Some(10000), Some(3));
    let end = start.elapsed();
    let _ = monitor::poster::post(String::from("finished")).await;
    println!("{:?}", end);

    Ok(())
}
