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
//     蒙特卡洛算法计算pi
//  */
// fn main() -> Result<()> {
//     let sc = Context::new()?;
//     let sz=1000000;
//     let col = sc.make_rdd((0..sz).collect::<Vec<_>>(), 2);
//     //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
//     // let y: f64 = rng.gen();
//     let coordinate_iter = col.map(Fn!(|i|{
//         let mut rng = rand::thread_rng();
//         // let y:f64 = rng.gen();
//         let pair=(rng.gen_range(-100.0,100.0) as f64,rng.gen_range(-100.0,100.0) as f64);
//         if pair.0*pair.0+pair.1*pair.1<=100.0*100.0 {
//             1
//         }else{
//             0
//         }
//     }));
//     let res = coordinate_iter.fold(0, Fn!(|acc,i|acc+i)).unwrap();
//     println!("result: {:?}", 1.0*res as f64*4.0/sz as f64);
//     Ok(())
// }

/*
    泰勒展开级数求e,使用高精度库num_bigfloat
 */
use vega::*;
use num_bigfloat::BigFloat;
use num_bigfloat::ONE;
use num_bigfloat::ZERO;
fn main() -> Result<()> {
    let sc = Context::new()?;
    let col = sc.make_rdd((0..700).collect::<Vec<_>>(), 3);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    // let y: f64 = rng.gen();
    let item_iter = col.map(Fn!(|i|{
        let mut frac=ONE.clone();
        for k in 1..=i{
            let f_k=BigFloat::from_i32(k);
            frac=frac.mul(&ONE.div(&f_k));
        }
        frac
    }));
    // let fraction = item_iter.map(Fn!(|i|1 as f64 / i as f64));
    let res=item_iter.fold(ZERO, Fn!(|acc,n|{
        (&acc as &BigFloat).add(&n)
    }));
    println!("result: {}", res.unwrap());
    Ok(())
}
