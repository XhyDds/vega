use std::time::Instant;
use vega::*;
mod benchmark;
fn main() -> Result<()> {
    let sc: std::sync::Arc<Context> = Context::new()?;
    let start = Instant::now();
    // benchmark::pi::calc_pi(&sc,Some(1000000),Some(2));
    benchmark::e::calc_e(&sc,Some(10000),Some(3));
    let end=start.elapsed();
    println!("{:?}",end);
    Ok(())
}
