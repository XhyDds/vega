use rand::Rng;
use vega::*;

#[allow(dead_code)]
pub fn multihead_attention(
    sc: &Arc<Context>,
    input_size: Option<i32>,
    heads_num: Option<i32>,
    num_slices: Option<usize>,
) {
    let start = Instant::now();
    let input_size = input_size.unwrap_or(10000);
    let heads_num = heads_num.unwrap_or(100);
    let num_slices = num_slices.unwrap_or(2);
    let k_weight = sc.make_rdd(0..input_size * heads_num, num_slices);
    let v = sc.make_rdd(0..heads_num, num_slices);
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

    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.

    let res = coordinate_iter.fold(0, Fn!(|acc, i| acc + i)).unwrap();
    println!("result: {:?}", (res as f64) * 4.0 / (param as f64));

    let end = start.elapsed();
    println!("in fn {:?}", end);
    // println!("result: {:?}", res);
}
