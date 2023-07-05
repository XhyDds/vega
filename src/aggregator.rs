use crate::serializable_traits::Data;
use crate::Fn;
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;

/** Aggregator for shuffle tasks.
* 这个Aggregator主要就是拿来装三个函数，用于处理Vec：
* 一个初始化函数，一个append函数，和一个拼接两Vec的函数
*/
#[derive(Serialize, Deserialize)]
pub struct Aggregator<K: Data, V: Data, C: Data> {
    #[serde(with = "serde_traitobject")]
    pub create_combiner: Box<dyn serde_traitobject::Fn(V) -> C + Send + Sync>,
    #[serde(with = "serde_traitobject")]
    pub merge_value: Box<dyn serde_traitobject::Fn((C, V)) -> C + Send + Sync>,
    #[serde(with = "serde_traitobject")]
    ///拼接两Vec的函数，用Box包装输入，传入两个Data类型的数据，返回一个Data类型的数据
    pub merge_combiners: Box<dyn serde_traitobject::Fn((C, C)) -> C + Send + Sync>,
    _marker: PhantomData<K>,
}

impl<K: Data, V: Data, C: Data> Aggregator<K, V, C> {
    pub fn new(
        create_combiner: Box<dyn serde_traitobject::Fn(V) -> C + Send + Sync>,
        merge_value: Box<dyn serde_traitobject::Fn((C, V)) -> C + Send + Sync>,
        merge_combiners: Box<dyn serde_traitobject::Fn((C, C)) -> C + Send + Sync>,
    ) -> Self {
        Aggregator {
            create_combiner,
            merge_value,
            merge_combiners,
            _marker: PhantomData,
        }
    }
}

impl<K: Data, V: Data> Default for Aggregator<K, V, Vec<V>> {
    fn default() -> Self {
        let merge_value = Box::new(Fn!(|mv: (Vec<V>, V)| {
            let (mut buf, v) = mv;
            buf.push(v);
            buf
        })); //往Vec里面添加值v，调用Vec<V>.push(V)

        let create_combiner = Box::new(Fn!(|v: V| vec![v])); //初始化，返回 vec![v]

        let merge_combiners = Box::new(Fn!(|mc: (Vec<V>, Vec<V>)| {
            let (mut b1, mut b2) = mc;
            b1.append(&mut b2);
            b1
        })); //把两个Vec<V>拼起来并返回
        Aggregator {
            create_combiner,
            merge_value,
            merge_combiners,
            _marker: PhantomData,
        }
    }
}
