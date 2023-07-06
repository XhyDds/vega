use crate::aggregator::Aggregator;
use crate::env;
use crate::partitioner::Partitioner;
use crate::rdd::RddBase;
use crate::serializable_traits::Data;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

// Revise if enum is good choice. Considering enum since down casting one trait object to another trait object is difficult.
#[derive(Clone, Serialize, Deserialize)]
pub enum Dependency {
    #[serde(with = "serde_traitobject")]
    NarrowDependency(Arc<dyn NarrowDependencyTrait>),
    #[serde(with = "serde_traitobject")]
    ShuffleDependency(Arc<dyn ShuffleDependencyTrait>),
}

pub trait NarrowDependencyTrait: Serialize + Deserialize + Send + Sync {
    fn get_parents(&self, partition_id: usize) -> Vec<usize>;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct OneToOneDependency {
    #[serde(with = "serde_traitobject")]
    rdd_base: Arc<dyn RddBase>,
}

impl OneToOneDependency {
    pub fn new(rdd_base: Arc<dyn RddBase>) -> Self {
        OneToOneDependency { rdd_base }
    }
}

impl NarrowDependencyTrait for OneToOneDependency {
    fn get_parents(&self, partition_id: usize) -> Vec<usize> {
        vec![partition_id]
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }
}

/// Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
/// 在Rust中，pub(crate)表示该项在当前crate中可见。这意味着该项可以被当前crate中的任何模块访问，但不能被crate外部的模块访问。4
// 在你提供的代码中，pub(crate)用于使RangeDependency结构体在当前crate中可见。
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RangeDependency {
    #[serde(with = "serde_traitobject")]
    rdd_base: Arc<dyn RddBase>,
    /// the start of the range in the child RDD
    out_start: usize,
    /// the start of the range in the parent RDD
    in_start: usize,
    /// the length of the range
    length: usize,
}

impl RangeDependency {
    pub fn new(
        rdd_base: Arc<dyn RddBase>,
        in_start: usize,
        out_start: usize,
        length: usize,
    ) -> Self {
        RangeDependency {
            rdd_base,
            in_start,
            out_start,
            length,
        }
    }
}

impl NarrowDependencyTrait for RangeDependency {
    fn get_parents(&self, partition_id: usize) -> Vec<usize> {
        if partition_id >= self.out_start && partition_id < self.out_start + self.length {
            vec![partition_id - self.out_start + self.in_start]
        } else {
            Vec::new()
        }
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }
}

pub trait ShuffleDependencyTrait: Serialize + Deserialize + Send + Sync {
    fn get_shuffle_id(&self) -> usize;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
    fn is_shuffle(&self) -> bool;
    fn do_shuffle_task(&self, rdd_base: Arc<dyn RddBase>, partition: usize) -> String;
}

//比较shuffle_id大小
impl PartialOrd for dyn ShuffleDependencyTrait {
    fn partial_cmp(&self, other: &dyn ShuffleDependencyTrait) -> Option<Ordering> {
        Some(self.get_shuffle_id().cmp(&other.get_shuffle_id()))
    }
}
impl PartialEq for dyn ShuffleDependencyTrait {
    fn eq(&self, other: &dyn ShuffleDependencyTrait) -> bool {
        self.get_shuffle_id() == other.get_shuffle_id()
    }
}
impl Eq for dyn ShuffleDependencyTrait {}
impl Ord for dyn ShuffleDependencyTrait {
    fn cmp(&self, other: &dyn ShuffleDependencyTrait) -> Ordering {
        self.get_shuffle_id().cmp(&other.get_shuffle_id())
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ShuffleDependency<K: Data, V: Data, C: Data> {
    pub shuffle_id: usize,
    pub is_cogroup: bool,
    #[serde(with = "serde_traitobject")]
    pub rdd_base: Arc<dyn RddBase>,
    #[serde(with = "serde_traitobject")]
    pub aggregator: Arc<Aggregator<K, V, C>>,
    #[serde(with = "serde_traitobject")]
    pub partitioner: Box<dyn Partitioner>,
    is_shuffle: bool,
}

impl<K: Data, V: Data, C: Data> ShuffleDependency<K, V, C> {
    pub fn new(
        shuffle_id: usize,
        is_cogroup: bool,
        rdd_base: Arc<dyn RddBase>,
        aggregator: Arc<Aggregator<K, V, C>>,
        partitioner: Box<dyn Partitioner>,
    ) -> Self {
        ShuffleDependency {
            shuffle_id,
            is_cogroup,
            rdd_base,
            aggregator,
            partitioner,
            is_shuffle: true,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> ShuffleDependencyTrait for ShuffleDependency<K, V, C> {
    fn get_shuffle_id(&self) -> usize {
        self.shuffle_id
    }

    fn is_shuffle(&self) -> bool {
        self.is_shuffle
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }

    fn do_shuffle_task(&self, rdd_base: Arc<dyn RddBase>, partition: usize) -> String {
        /*ChatGpt:
            这个函数是一个执行Shuffle任务的方法，接受一个RDD对象和一个分区索引作为参数，返回一个字符串表示Shuffle任务的服务器URI。
            该方法首先从RDD中获取指定分区的Split，然后遍历该Split中的所有元素，并根据键值对的键使用Partitioner获取对应的Bucket ID。
            然后，该方法将键值对添加到相应的Bucket中，如果Bucket中已经存在键，则调用Aggregator的merge_value方法将新值与旧值合并，否则调用create_combiner方法创建一个新的组合器。
            最后，该方法将每个Bucket中的键值对序列化为字节数组，并将其存储在环境变量的SHUFFLE_CACHE中，其中键是Shuffle ID、分区索引和Bucket ID的元组，值是字节数组。
            该方法返回Shuffle任务的服务器URI，以便客户端可以向其发送请求以获取Shuffle数据。
            Executes a shuffle task for the given RDD partition and returns the server URI.
            @Arguments
            rdd_base - A reference-counted trait object that represents the RDD.
            partition - The index of the partition to perform the shuffle task on.
            @Returns
            A string representing the server URI for the shuffle task.
            @Examples
            use std::sync::Arc;
            use rdd::*;
            // create an RDD
            let rdd = sc.parallelize(vec![("a", 1), ("b", 2), ("c", 3)], 2);
            // create a shuffle dependency
            let shuffle_dep = ShuffleDependency::new(Arc::new(rdd.clone()), Box::new(|x| x as u64 % 2), 2);
            // create a shuffle map task
            let shuffle_map_task = ShuffleMapTask::new(0, Arc::new(shuffle_dep), Box::new(SumAggregator::new()));
            // execute the shuffle map task for partition 0
            let server_uri = shuffle_map_task.do_shuffle_task(Arc::new(rdd), 0);
        */
        log::debug!(
            "executing shuffle task #{} for partition #{}",
            self.shuffle_id,
            partition
        );
        let split = rdd_base.splits()[partition].clone(); //获取指定分区的split（reduce_id）
        let aggregator = self.aggregator.clone();
        let num_output_splits = self.partitioner.get_num_of_partitions(); //以下注释里面称为n
        log::debug!("is cogroup rdd: {}", self.is_cogroup);
        log::debug!("number of output splits: {}", num_output_splits);
        let partitioner = self.partitioner.clone();
        let mut buckets: Vec<HashMap<K, C>> = (0..num_output_splits)
            .map(|_| HashMap::new())
            .collect::<Vec<_>>(); //[n个空HashMap]的数组
        log::debug!(
            "before iterating while executing shuffle map task for partition #{}",
            partition
        );
        log::debug!("split index: {}", split.get_index());

        //到这一步，我们有了分区号partition，分区数n=num_output_splits，reduce_id(split)，空桶buckets
        let iter = if self.is_cogroup {
            rdd_base.cogroup_iterator_any(split)
        } else {
            rdd_base.iterator_any(split.clone())
            //若在shuffle_rdd里，split就是reduce_id，这句会得到对应的shuffle文件读出的（K,V）对
        };

        //下面的代码，是把iter里面的所有(K,V)对，按照K的hash值，将V分配到n个HashMap里面，最后插进内存(self.shuffle_id, partition, i)处(i是桶编号)
        for (count, i) in iter.unwrap().enumerate() {
            let b = i.into_any().downcast::<(K, V)>().unwrap();
            let (k, v) = *b;
            if count == 0 {
                log::debug!(
                    "iterating inside dependency map task after downcasting: key: {:?}, value: {:?}",
                    k,
                    v
                );
            }
            let bucket_id = partitioner.get_partition(&k); //这里key被hash了一遍
            let bucket = &mut buckets[bucket_id];
            if let Some(old_v) = bucket.get_mut(&k) {
                let input = ((old_v.clone(), v),);
                let output = aggregator.merge_value.call(input);
                *old_v = output;
            } else {
                bucket.insert(k, aggregator.create_combiner.call((v,)));
            } //把所有v的内容hash到各个桶里
        }
        //这时得到的映射为：i->Vec<(K, V)>的HashMap，i为桶的编号，hash(K)=i=reduce_id

        for (i, bucket) in buckets.into_iter().enumerate() {
            let set: Vec<(K, C)> = bucket.into_iter().collect();
            let ser_bytes = bincode::serialize(&set).unwrap();
            log::debug!(
                "shuffle dependency map task set from bucket #{} in shuffle id #{}, partition #{}: {:?}",
                i,
                self.shuffle_id,
                partition,
                set.get(0)
            );
            if env::Configuration::get().is_sort_shuffle {
                if let Some(mut old_v) = env::SHUFFLE_CACHE.get_mut(&(self.shuffle_id, i)) {
                    let mut new_v = &mut old_v;
                    (*new_v).push(ser_bytes);
                } else {
                    env::SHUFFLE_CACHE.insert((self.shuffle_id, i), vec![ser_bytes]);
                }
            } else {
                //env::SHUFFLE_CACHE.insert((self.shuffle_id, partition, i), ser_bytes);
            }
        }
        log::debug!(
            "returning shuffle address for shuffle task #{}",
            self.shuffle_id
        );
        env::Env::get().shuffle_manager.get_server_uri()
    }
}
