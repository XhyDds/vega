use rand::Rng;
use std::sync::Arc;
use std::time::Instant;
use vega::*;

#[allow(dead_code, unused_braces)]
pub fn multihead_attention(sc: &Arc<Context>, num_slices: Option<usize>) {
    const INPUT_SIZE: usize = 10000;
    const HEADS_NUM: usize = 100;
    let num_slices = num_slices.unwrap_or(2);
    let mut k_weights = Vec::new();
    let mut v = Vec::new();

    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    //Fn<Fn<impl Fn(&Fn<()>, ({unknown},)) -> i32>>这种可以运行
    //Fn<Fn<&usize, impl Fn(&Fn<&usize, ()>, ({unknown},)) -> Arc<Vec<f64>>>>
    let mut rng = rand::thread_rng();
    for _ in 0..HEADS_NUM {
        let mut k_weight: Vec<f64> = Vec::with_capacity(INPUT_SIZE);
        for _ in 0..INPUT_SIZE {
            k_weight.push(rng.gen_range(-1.0f64, 1.0f64) as f64)
        }
        k_weights.push(k_weight);
        v.push(rng.gen_range(-1.0f64, 1.0f64) as f64)
    }
    let k_weights_param = sc.parallelize(k_weights, num_slices); //获得k_weight的参数
    let v_param = sc.parallelize(v, num_slices); //获得v的参数

    let start = Instant::now();
    for _ in 0..1 {
        let mut rng = rand::thread_rng();
        let mut input = Vec::with_capacity(INPUT_SIZE);
        for _ in 0..INPUT_SIZE {
            input.push(rng.gen_range(-1.0f64, 1.0f64) as f64);
        }
        //let start = Instant::now();
        //let query = sc.parallelize(input, num_slices);
        //let query_key_pair=query.zip(Arc::new(k_weights_param));
        let input_ref = Arc::new(input);
        let mut input_ref_set = Vec::new();
        for _ in 0..HEADS_NUM {
            input_ref_set.push(input_ref.clone());
        }
        //let input_rdd = sc.parallelize(input, num_slices);
        /*let input_key_pair = input_rdd.zip(Arc::new(k_weights_param));
        let dot_fn = Fn!(|q_k: (Vec<f64>, Vec<f64>)| {
            let sum = 0.0f64;
            for i in 0..INPUT_SIZE {
                sum += q_k.0[i] * q_k.1[i];
            }
            sum
        });*/
        let input_ref_rdd = sc.parallelize(input_ref_set, num_slices);
        let input_key_pair = input_ref_rdd.zip(Arc::new(k_weights_param.clone()));
        let dot_fn = Fn!(|q_k: (Arc<Vec<f64>>, Vec<f64>)| {
            let mut sum = 0.0f64;
            for i in 0..INPUT_SIZE {
                sum += q_k.0[i] * q_k.1[i];
            }
            sum
        });
        let query_res = input_key_pair.map(dot_fn).collect().unwrap();
        let mut query_res = query_res as Vec<f64>;

        //let max_query_res = query_res.fold(0.0f64, Fn!(|acc, i| acc.max(i))).unwrap();
        //let max_query_res=
        let mut max_query_res = query_res[0];
        for i in 0..HEADS_NUM {
            max_query_res = max_query_res.max(query_res[i]);
        }
        for i in 0..HEADS_NUM {
            query_res[i] = (query_res[i] - max_query_res).exp();
        }

        //let query_res_sub = query_res.map(Fn!(|q_res| { q_res - max_query_res }));
        //let query_res_softmax = query_res_sub.map(Fn!(|q_res| { q_res.exp() }));
        let query_res_softmax = sc.parallelize(query_res, num_slices);
        let weight_v_pair = query_res_softmax.zip(Arc::new(v_param.clone()));
        let res_vec = weight_v_pair.map(Fn!(|w_v: (f64, f64)| { w_v.0 * w_v.1 }));
        let res = res_vec.fold(0.0f64, Fn!(|acc, i| acc + i)).unwrap();
        println!("result: {:?}", res);
    }

    let end = start.elapsed();
    println!("Multihead_attention_time_cost: {:?}", end);
}
