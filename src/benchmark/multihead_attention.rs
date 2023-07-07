use rand::Rng;
use std::sync::Arc;
use std::time::Instant;
use vega::*;
/*
#[allow(dead_code)]
pub fn multihead_attention(
    sc: &Arc<Context>,
    input_size: Option<usize>,
    heads_num: Option<usize>,
    num_slices: Option<usize>,
) {
    let input_size = input_size.unwrap_or(10000);
    let heads_num = heads_num.unwrap_or(100);
    let num_slices = num_slices.unwrap_or(2);
    let k_weights = sc.make_rdd(0..heads_num, num_slices);
    let v = sc.make_rdd(0..heads_num, num_slices);

    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let k_weights_param = k_weights.map(Fn!(|_| {
        let mut rng = rand::thread_rng();
        let k_weight = Vec::with_capacity(input_size);
        for i in 0..input_size {
            k_weight.push(rng.gen_range(-1.0f64, 1.0f64) as f64)
        }
        k_weight
    })); //获得k_weight的参数
    let start = Instant::now();
    let v_param = v.map(Fn!(|_| {
        let mut rng = rand::thread_rng();
        rng.gen_range(-1.0f64, 1.0f64) as f64
    })); //获得v的参数

    for i in 0..100 {
        let mut rng = rand::thread_rng();
        let input = Vec::with_capacity(input_size);
        for _ in 0..input_size {
            input.push(rng.gen_range(-1.0f64, 1.0f64) as f64);
        }
        //let start = Instant::now();

        //let query = sc.parallelize(input, num_slices);
        //let query_key_pair=query.zip(Arc::new(k_weights_param));
        let query_res = k_weights_param.map(Fn!(|k| {
            let sum = 0.0f64;
            for i in 0..input_size {
                sum += k[i] * input[i];
            }
            sum
        }));
        let max_query_res = query_res.max();
        let query_res_sub = query_res.map(Fn!(|q_res| { q_res - max_query_res }));
        let query_res_softmax = query_res_sub.map(Fn!(|q_res| { q_res.exp() }));
        let weight_v_pair = query_res_softmax.zip(Arc::new(v_param));
        let res_vec = weight_v_pair.map(Fn!(|(w, v)| { w * v }));
        let res = res_vec.fold(0, Fn!(|acc, i| acc + i)).unwrap();
        println!("result: {:?}", res);
    }

    let end = start.elapsed();
    println!("in fn {:?}", end);
    // println!("result: {:?}", res);
}
*/
