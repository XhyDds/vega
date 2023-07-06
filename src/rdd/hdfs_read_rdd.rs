//! This module implements parallel collection RDD for dividing the input collection for parallel processing.
use std::io::{BufReader, Read};
use std::sync::{Arc, Weak};

use crate::context::Context;
use crate::dependency::Dependency;
use crate::error::Result;
use crate::rdd::{Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data};
use crate::split::Split;
use crate::hosts::Hosts;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_derive::{Serialize};
use hdrs::Client;

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
pub struct HdfsReadRddSplit {
    rdd_id: i64,
    index: usize,
    nn: String,
    values: Vec<String>,
}

impl Split for HdfsReadRddSplit {
    fn get_index(&self) -> usize {
        self.index
    }
}

impl HdfsReadRddSplit {
    fn new(rdd_id: i64, nn: String, index: usize, values: Vec<String>) -> Self {
        HdfsReadRddSplit {
            rdd_id,
            nn,
            index,
            values,
        }
    }
    // Lot of unnecessary cloning is there. Have to refactor for better performance
    fn iterator(&self) -> Box<dyn Iterator<Item = Vec<u8>>> {
        let data = self.values.clone();
        let len = data.len();
        let mut res = Vec::with_capacity(len);
        for path in data {
            let fs = Client::connect(self.nn.as_str()).unwrap();
            let file = fs
                .open_file()
                .read(true)
                .open(path.as_str())
                .unwrap();
            let mut content = vec![];
            let mut reader = BufReader::new(file);
            reader.read_to_end(&mut content).unwrap();
            res.push(content);
        }
        println!("finished reading files");
        Box::new(res.into_iter())
    } 
}

/// 结构体HdfsReadRddVals
/// 成员：
/// RddVals: Rdd的元数据
/// splits_: 分区
/// num_slices: 分区数量
/// context: 环境/上下文(接受一个弱引用)
#[derive(Serialize, Deserialize)]
pub struct HdfsReadRddVals {
    vals: Arc<RddVals>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Weak<Context>,
    splits_: Vec<Vec<String>>,
    num_slices: usize,
}

#[derive(Serialize, Deserialize)]
pub struct HdfsReadRdd {
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    nn: String,
    path: String,
    rdd_vals: Arc<HdfsReadRddVals>,
}

impl Clone for HdfsReadRdd {
    fn clone(&self) -> Self {
        HdfsReadRdd {
            name: Mutex::new(self.name.lock().clone()),
            nn: self.nn.clone(),
            path: self.path.clone(),
            rdd_vals: self.rdd_vals.clone(),
        }
    }
}

/// 函数HdfsReadRdd::new
/// 接收一个迭代器data和分区数量num_slices
/// 产生一个HdfsReadRdd对象
/// HdfsReadRdd对象包含一个Mutex<String>和一个Arc<HdfsReadRddVals<T>>
///
impl HdfsReadRdd {
    pub fn new(context: Arc<Context>, path: String, num_slices: usize) -> Self
    {
        let nn = match Hosts::get() {
            //namenode默认是master
            Ok(hosts) => {
                let mut res = hosts.master.to_string();
                let pos = res.find(':');
                match pos {
                    Some(pos) => {
                        res.replace_range(pos.., ":9000");
                        res
                    }
                    None => res,
                }
            }
            //Ok(_) => String::from("192.168.179.129:9000"),
            Err(_) => String::from("localhost:9000"),
        };

        let nn_c = nn.clone();
        let path_c = path.clone();

        HdfsReadRdd {
            name: Mutex::new("parallel_collection".to_owned()),
            nn,
            path,
            rdd_vals: Arc::new(HdfsReadRddVals {
                //downgrade()方法返回一个Weak<T>类型的对象，Weak<T>是一个弱引用，不会增加引用计数
                context: Arc::downgrade(&context),
                //由context生成rdd_id
                vals: Arc::new(RddVals::new(context.clone())),
                //由data生成的分区
                splits_: HdfsReadRdd::slice(nn_c.as_str(), path_c.as_str(), num_slices),
                //分区数
                num_slices,
            }),
        }
    }

    /**
     * slice 接收data和分区数量 num_slices
     * 消耗掉data中的元素，产生一堆内存中分区对象
     * 生成将data分成num_slices个分区
     */
    fn slice(nn: &str, path: &str, num_slices: usize) -> Vec<Vec<String>>
    {
        let mut num_slices =  num_slices;
        if num_slices < 1 {
            num_slices = 1;
        } 
        let fs = Client::connect(nn).expect("cannot connect to namenode");
        let metadata = fs.metadata(path).expect("cannot get metadata");
        let is_file = metadata.is_file();
        if is_file {
            vec![vec![path.to_string()]]
        }
        else {
            let dir_entries = fs.read_dir(path).expect("cannot read dir").into_inner();
            if num_slices < dir_entries.len() {
                num_slices = dir_entries.len();
            }
            let mut res = Vec::with_capacity(num_slices);
            for _ in 0..num_slices {
                res.push(Vec::<String>::new());
            }
            for (i, entry) in dir_entries.enumerate() {
                let path = entry.path();
                let index = i % num_slices;
                res[index].push(path.to_string());
            }
            res
        }
    }
}

// impl RddBase for HdfsReadRdd 
// {
//     fn cogroup_iterator_any(
//         &self,
//         split: Box<dyn Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
//         log::debug!("inside iterator_any parallel collection",);
//         Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
//             Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
//         })))
//     }
// }

impl RddBase for HdfsReadRdd {
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
                Box::new(HdfsReadRddSplit::new(
                    self.rdd_vals.vals.id as i64,
                    self.nn.clone(),
                    i,
                    self.rdd_vals.splits_[i as usize].to_vec(),
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

impl Rdd for HdfsReadRdd {
    type Item = Vec<u8>;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(HdfsReadRdd {
            name: Mutex::new(self.name.lock().clone()),
            nn: self.nn.clone(),
            path: self.path.clone(),
            rdd_vals: self.rdd_vals.clone(),
        })
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        if let Some(s) = split.downcast_ref::<HdfsReadRddSplit>() {
            Ok(s.iterator())
        } else {
            panic!(
                "Got split object from different concrete type other than HdfsReadRddSplit"
            )
        }
    }
}
