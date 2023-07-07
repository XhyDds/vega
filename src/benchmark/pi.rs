use rand::Rng;
use std::sync::Arc;
use std::time::Instant;
use vega::*;
/// 蒙特卡洛方法计算pi
/// param(Option<i32>) 是随机试验次数
/// num_slices(Option<usize>)是rdd分区数量
/// 这里使用Context的引用传参，如果直接传入会大大损失性能
/// 2.971s->1.130s
#[allow(dead_code)]
pub fn calc_pi(sc: &Arc<Context>, param: Option<i32>, num_slices: Option<usize>) {
    let start = Instant::now();
    let param = param.unwrap_or(100000);
    let num_slices = num_slices.unwrap_or(2);
    let col = sc.make_rdd(0..param, num_slices);

    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let coordinate_iter = col.map(Fn!(|_| {
        let mut rng = rand::thread_rng();
        let pair = (
            rng.gen_range(-100.0f64, 100.0f64) as f64,
            rng.gen_range(-100.0f64, 100.0f64) as f64,
        );
        if pair.0 * pair.0 + pair.1 * pair.1 <= 100.0 * 100.0 {
            1
        } else {
            0
        }
    }));
    let res = coordinate_iter.fold(0, Fn!(|acc, i| acc + i)).unwrap();
    println!("result: {:?}", (res as f64) * 4.0 / (param as f64));

    // benchmark::pi::calc_pi(sc,Some(1000000),Some(2));
    let end = start.elapsed();
    println!("in fn {:?}", end);
    // println!("result: {:?}", res);
}
