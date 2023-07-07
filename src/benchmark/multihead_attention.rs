use rand::Rng;
use std::sync::Arc;
use std::time::Instant;
use vega::*;

#[allow(dead_code, unused_braces)]
pub fn multihead_attention(sc: &Arc<Context>, num_slices: Option<usize>) {
    const INPUT_SIZE: usize = 10000;
    const HEADS_NUM: usize = 100;
    let num_slices = num_slices.unwrap_or(2);
    let k_weights = sc.make_rdd(0..(HEADS_NUM as i32), num_slices);
    let v = sc.make_rdd(0..(HEADS_NUM as i32), num_slices);

    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    //Fn<Fn<impl Fn(&Fn<()>, ({unknown},)) -> i32>>这种可以运行
    //Fn<Fn<&usize, impl Fn(&Fn<&usize, ()>, ({unknown},)) -> Arc<Vec<f64>>>>
    let gen_k_weight = Fn!(|_| {
        let mut rng = rand::thread_rng();
        let mut k_weight: Vec<f64> = Vec::with_capacity(INPUT_SIZE);
        for i in 0..INPUT_SIZE {
            k_weight.push(rng.gen_range(-1.0f64, 1.0f64) as f64)
        }
        k_weight
    });
    let k_weights_param = k_weights.map(gen_k_weight).collect()?; //获得k_weight的参数
    let start = Instant::now();
    let v_param = v.map(Fn!(|_| {
        let mut rng = rand::thread_rng();
        rng.gen_range(-1.0f64, 1.0f64) as f64
    })); //获得v的参数

    for i in 0..100 {
        let mut rng = rand::thread_rng();
        let input = Vec::with_capacity(INPUT_SIZE);
        for _ in 0..INPUT_SIZE {
            input.push(rng.gen_range(-1.0f64, 1.0f64) as f64);
        }
        //let start = Instant::now();

        //let query = sc.parallelize(input, num_slices);
        //let query_key_pair=query.zip(Arc::new(k_weights_param));
        let input_rdd = sc.parallelize(input, num_slices);
        let input_key_pair = input_rdd.zip(Arc::new(k_weights_param));
        let dot_fn = Fn!(|q_k| {
            let sum = 0.0f64;
            for i in 0..INPUT_SIZE {
                sum += q_k.0[i] * q_k.1[i];
            }
            sum
        });
        let query_res = input_key_pair.map(dot_fn);
        let max_query_res = query_res.max().unwrap().unwrap() as f64;
        let query_res_sub = query_res.map(Fn!(|q_res| { q_res - max_query_res }));
        let query_res_softmax = query_res_sub.map(Fn!(|q_res| { q_res.exp() }));
        let weight_v_pair = query_res_softmax.zip(Arc::new(v_param));
        let res_vec = weight_v_pair.map(Fn!(|w_v: (f64, f64)| { w_v.0 * w_v.1 }));
        let res = res_vec.fold(0, Fn!(|acc, i| acc + i)).unwrap();
        println!("result: {:?}", res);
    }

    let end = start.elapsed();
    println!("in fn {:?}", end);
    // println!("result: {:?}", res);
}
