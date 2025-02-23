//! This module implements parallel collection RDD for dividing the input collection for parallel processing.
use std::sync::{Arc, Weak};

use crate::context::Context;
use crate::dependency::Dependency;
use crate::error::Result;
use crate::rdd::{Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data};
use crate::split::Split;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_derive::{Serialize};

/// A collection of objects which can be sliced into partitions with a partitioning function.
pub trait Chunkable<D>
where
    D: Data,
{
    fn slice_with_set_parts(self, parts: usize) -> Vec<Arc<Vec<D>>>;

    fn slice(self) -> Vec<Arc<Vec<D>>>
    where
        Self: Sized,
    {
        let as_many_parts_as_cpus = num_cpus::get();
        self.slice_with_set_parts(as_many_parts_as_cpus)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ParallelCollectionSplit<T> {
    rdd_id: i64,
    index: usize,
    values: Arc<Vec<T>>,
}

impl<T: Data> Split for ParallelCollectionSplit<T> {
    fn get_index(&self) -> usize {
        self.index
    }
}

impl<T: Data> ParallelCollectionSplit<T> {
    fn new(rdd_id: i64, index: usize, values: Arc<Vec<T>>) -> Self {
        ParallelCollectionSplit {
            rdd_id,
            index,
            values,
        }
    }
    // Lot of unnecessary cloning is there. Have to refactor for better performance
    // fn iterator(&self) -> Box<dyn Iterator<Item = T>> {
    //     let data = self.values.clone();
    //     let len = data.len();
    //     Box::new((0..len).map(move |i| data[i].clone()))
    // }


    fn iterator(&self) -> Box<dyn Iterator<Item = T>> 
    {
        //这里使用to_owned会产生clone，但是相较于原版减少一次
        // println!("new iter");
        Box::new(self.values[0..].to_owned().into_iter())
    }
}

/// 结构体ParallelCollectionVals
/// 成员：
/// RddVals: Rdd的元数据
/// splits_: 分区
/// num_slices: 分区数量
/// context: 环境/上下文(接受一个弱引用)
#[derive(Serialize, Deserialize)]
pub struct ParallelCollectionVals<T> {
    vals: Arc<RddVals>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Weak<Context>,
    splits_: Vec<Arc<Vec<T>>>,
    num_slices: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollection<T> {
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    rdd_vals: Arc<ParallelCollectionVals<T>>,
}

impl<T: Data> Clone for ParallelCollection<T> {
    fn clone(&self) -> Self {
        ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
        }
    }
}

/// 函数ParallelCollection::new
/// 接收一个迭代器data和分区数量num_slices
/// 产生一个ParallelCollection对象
/// ParallelCollection对象包含一个Mutex<String>和一个Arc<ParallelCollectionVals<T>>
///
impl<T: Data> ParallelCollection<T> {
    pub fn new<I>(context: Arc<Context>, data: I, num_slices: usize) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        ParallelCollection {
            name: Mutex::new("parallel_collection".to_owned()),
            rdd_vals: Arc::new(ParallelCollectionVals {
                //downgrade()方法返回一个Weak<T>类型的对象，Weak<T>是一个弱引用，不会增加引用计数
                context: Arc::downgrade(&context),
                //由context生成rdd_id
                vals: Arc::new(RddVals::new(context.clone())),
                //由data生成的分区
                splits_: ParallelCollection::slice(data, num_slices),
                //分区数
                num_slices,
            }),
        }
    }

    pub fn from_chunkable<C>(context: Arc<Context>, data: C) -> Self
    where
        C: Chunkable<T>,
    {
        let splits_ = data.slice();
        let rdd_vals = ParallelCollectionVals {
            context: Arc::downgrade(&context),
            vals: Arc::new(RddVals::new(context.clone())),
            num_slices: splits_.len(),
            splits_,
        };
        ParallelCollection {
            name: Mutex::new("parallel_collection".to_owned()),
            rdd_vals: Arc::new(rdd_vals),
        }
    }

    /**
     * slice 接收data和分区数量 num_slices
     * 消耗掉data中的元素，产生一堆内存中分区对象
     * 生成将data分成num_slices个分区
     */
    fn slice<I>(data: I, num_slices: usize) -> Vec<Arc<Vec<T>>>
    where
        I: IntoIterator<Item = T>,
    {
        if num_slices < 1 {
            panic!("Number of slices should be greater than or equal to 1");
        } else {
            let mut slice_count = 0;
            let data: Vec<_> = data.into_iter().collect();
            // data是迭代器类型，迭代器的适配器分为消费者适配器和迭代器适配器
            // 消费者适配器调用next方法会消耗掉元素，立即就要使用元素,例如collect
            // 迭代器适配器例如filter,map不会消耗，只是表示一种关系，是懒运算
            let data_len = data.len();
            let mut end = ((slice_count + 1) * data_len) / num_slices;
            let mut output = Vec::new();
            let mut tmp = Vec::new();
            let mut iter_count = 0;
            // 将data中的元素放入tmp中，当tmp中的元素数量达到end时，将tmp放入output中
            for i in data {
                if iter_count < end {
                    tmp.push(i);
                    iter_count += 1;
                } else {
                    // tmp中的元素数量达到end,放入output中,并更新end,开启新的分区的收集
                    slice_count += 1;
                    end = ((slice_count + 1) * data_len) / num_slices;
                    output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
                    tmp.push(i);
                    iter_count += 1;
                }
            }
            output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
            output
        }
    }
}

impl<K: Data, V: Data> RddBase for ParallelCollection<(K, V)> 
{
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any parallel collection",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<T: Data> RddBase for ParallelCollection<T> {
    fn get_rdd_id(&self) -> usize {
        self.rdd_vals.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.rdd_vals.vals.context.upgrade().unwrap()
    }

    fn get_op_name(&self) -> String {
        self.name.lock().to_owned()
    }

    fn register_op_name(&self, name: &str) {
        let own_name = &mut *self.name.lock();
        *own_name = name.to_owned();
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.rdd_vals.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.rdd_vals.splits_.len())
            .map(|i| {
                Box::new(ParallelCollectionSplit::new(
                    self.rdd_vals.vals.id as i64,
                    i,
                    self.rdd_vals.splits_[i as usize].clone(),
                )) as Box<dyn Split>
            })
            .collect::<Vec<Box<dyn Split>>>()
    }

    fn number_of_splits(&self) -> usize {
        self.rdd_vals.splits_.len()
    }

    default fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        self.iterator_any(split)
    }

    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any parallel collection",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }
}

impl<T: Data> Rdd for ParallelCollection<T> {
    type Item = T;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
        })
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        if let Some(s) = split.downcast_ref::<ParallelCollectionSplit<T>>() {
            Ok(s.iterator())
        } else {
            panic!(
                "Got split object from different concrete type other than ParallelCollectionSplit"
            )
        }
    }
}
