use std::time::Instant;

// use vega::*;

// fn main() -> Result<()> {
//     let sc = Context::new()?;
//     let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
//     //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
//     let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
//     let res = vec_iter.collect().unwrap();
//     println!("result: {:?}", res);
//     Ok(())
// }

// use rand::Rng;
// use vega::*;
// /*
//    蒙特卡洛算法计算pi
// */
// fn main() -> Result<()> {
//     let pstart=Instant::now();
//     let sc = Context::new()?;

//     let start = Instant::now();

//     let sz=1000000;
//     let col = sc.make_rdd(0..sz, 2);
//     //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
//     // let y: f64 = rng.gen();
//     let coordinate_iter = col.map(Fn!(|_| {
//         let mut rng = rand::thread_rng();
//         // let y:f64 = rng.gen();
//         let pair = (
//             rng.gen_range(-100.0, 100.0) as f64,
//             rng.gen_range(-100.0, 100.0) as f64,
//         );
//         if pair.0 * pair.0 + pair.1 * pair.1 <= 100.0 * 100.0 {
//             1
//         } else {
//             0
//         }
//     }));
//     let res = coordinate_iter.fold(0, Fn!(|acc, i| acc + i)).unwrap();
//     println!("result: {:?}", 1.0 * res as f64 * 4.0 / sz as f64);

//     let end = start.elapsed();
//     println!("运算时间:{:?}",end);
//     let pend=pstart.elapsed();
//     println!("程序运行总时间:{:?}",pend);
//     Ok(())
// }

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
