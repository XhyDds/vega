use std::sync::Arc;

use num_bigfloat::BigFloat;
use vega::*;
/// param(Option<i32>):级数项数
/// num_slices(Option<usize>):分区数量
/// 通过泰勒级数计算e，可以精确到后100位，当前使用大数库num_bigfloat使用128位，后续可以考虑替换其他
/// 更高精度的库函数，但是要注意是否可以支持序列化。
/// 这里使用Context的引用传参，如果直接传入会大大损失性能
/// 6.114s->4.627s
#[allow(dead_code)]
pub fn calc_e(sc: &Arc<Context>, param: Option<i32>, num_slices: Option<usize>) {
    use num_bigfloat::ZERO;
    let col = sc.make_rdd(
        (0..param.unwrap_or(10000)).collect::<Vec<_>>(),
        num_slices.unwrap_or(3),
    );
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let item_iter = col.map(Fn!(|i| {
        // let zero=BigFloat::from_u8(0);
        let mut one = BigFloat::from_u8(1);
        let mut res = BigFloat::from_u8(1);
        for _ in 1..=i {
            res = (&res).div(&one);
            one = (&one).add(&(BigFloat::from_u8(1)));
        }
        res
    }));
    let ans = item_iter.fold(ZERO, Fn!(|acc, i| (&acc as &BigFloat).add(&i)));
    println!("{}", ans.unwrap());
}
