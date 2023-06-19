use vega::*;
fn main() -> Result<()> {
    let sc = Context::new()?;
    let col = sc.make_rdd((0..100).collect::<Vec<_>>(), 3);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    // let y: f64 = rng.gen();
    let item_iter = col.map(Fn!(|i|{
        (1..=i).product::<i32>()
    }));
    let fraction = item_iter.map(Fn!(|i|1 as f64 / i as f64));
    let res=fraction.fold(0.0, Fn!(|acc,n|{
        acc+n
    }));
    println!("result: {:?}", res);
    Ok(())
}
