use rand::Rng;
use std::sync::Arc;
use std::time::Instant;
use vega::*;

pub fn main(){
    let sc = Context::new().unwrap();
    let mut rng = rand::thread_rng();
    
    let mut vec = vec![(0, 0)];
    for _ in 0..1000000 {
        vec.push((rng.gen_range(0i32, 100i32), rng.gen_range(0i32, 100i32)));
    }
    let start = Instant::now();
    let r = sc.make_rdd(vec, 4);
    let g = r.group_by_key(4);
    let res = g.collect().unwrap();
    //println!("result: {:?}", res);
    let end = start.elapsed();
    println!("group_by_time_cost: {:?}", end);
}
