// use std::marker::PhantomData;
// use std::net::{Ipv4Addr, SocketAddrV4};
// use std::sync::{atomic::AtomicBool, atomic::Ordering::SeqCst, Arc};

// use crate::context::Context;
// use crate::dependency::{Dependency, OneToOneDependency};
// use crate::error::Result;
// use crate::hosts::Hosts;
// use crate::rdd::{Rdd, RddBase, RddVals};
// use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
// use crate::split::Split;
// use parking_lot::Mutex;
// use serde_derive::{Deserialize, Serialize};

// #[derive(Serialize, Deserialize)]
// pub struct HdfsRdd<T: Data>
// {
//     #[serde(skip_serializing, skip_deserializing)]
//     name: Mutex<String>,
//     #[serde(with = "serde_traitobject")]
//     nn: String,
//     path: String,
//     splits: Vec<SocketAddrV4>,
//     vals: Arc<RddVals>,
//     pinned: AtomicBool,
//     _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
// }

// // Can't derive clone automatically
// impl<T: Data> Clone for HdfsRdd<T>
// {
//     fn clone(&self) -> Self {
//         HdfsRdd {
//             name: Mutex::new(self.name.lock().clone()),
//             nn: self.nn.clone(),
//             path: self.path.clone(),
//             splits: self.splits.clone(),
//             vals: self.vals.clone(),
//             pinned: AtomicBool::new(self.pinned.load(SeqCst)),
//             // 一个标记，用于强制标记类型参数T
//             _marker_t: PhantomData,
//         }
//     }
// }

// impl<T: Data> HdfsRdd<T>

// {
//     /// mapper_rdd的构造函数
//     /// 接受rdd数据和函数f
//     /// 返回
//     pub(crate) fn new(context: Arc<Context>, path: String) -> Self {
//         let mut vals = RddVals::new(context);
//         let vals = Arc::new(vals);
//         let nn = match Hosts::get() {
//             //namenode默认是master
//             Ok(hosts) => {
//                 let mut res = hosts.master.to_string();
//                 let pos = res.find(':');
//                 match pos {
//                     Some(pos) => {
//                         res.replace_range(pos.., ":9000");
//                         res
//                     }
//                     None => res,
//                 }
//             }
//             Err(_) => String::from("localhost:9000"),
//         };
//         HdfsRdd {
//             name: Mutex::new("hdfs".to_owned()),
//             nn,
//             path,
//             splits: context.address_map.clone(),
//             vals,
//             pinned: AtomicBool::new(false),
//             _marker_t: PhantomData,
//         }
//     }

//     pub(crate) fn pin(self) -> Self {
//         self.pinned.store(true, SeqCst);
//         self
//     }
// }

// impl<T: Data> RddBase for HdfsRdd<T>

// {
//     fn get_rdd_id(&self) -> usize {
//         self.vals.id
//     }

//     fn get_context(&self) -> Arc<Context> {
//         self.vals.context.upgrade().unwrap()
//     }

//     fn get_op_name(&self) -> String {
//         self.name.lock().to_owned()
//     }

//     fn register_op_name(&self, name: &str) {
//         let own_name = &mut *self.name.lock();
//         *own_name = name.to_owned();
//     }

//     fn get_dependencies(&self) -> Vec<Dependency> {
//         vec![]
//     }

//     fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
//         let split = split.downcast_ref::<SocketAddrV4>().unwrap();
//         vec![split.ip()]
//     }

//     fn splits(&self) -> Vec<Box<dyn Split>> {
//         self.prev.splits()
//     }

//     fn number_of_splits(&self) -> usize {
//         self.prev.number_of_splits()
//     }

//     default fn cogroup_iterator_any(
//         &self,
//         split: Box<dyn Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
//         self.iterator_any(split)
//     }

//     default fn iterator_any(
//         &self,
//         split: Box<dyn Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
//         log::debug!("inside iterator_any maprdd",);
//         Ok(Box::new(
//             self.iterator(split)?
//                 .map(|x| Box::new(x) as Box<dyn AnyData>),
//         ))
//     }

//     fn is_pinned(&self) -> bool {
//         self.pinned.load(SeqCst)
//     }
// }

// impl RddBase for HdfsRdd<T>
// {
//     fn cogroup_iterator_any(
//         &self,
//         split: Box<dyn Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
//         log::debug!("inside iterator_any maprdd",);
//         Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
//             Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
//         })))
//     }
// }

// impl Rdd for HdfsRdd<T>

// {
//     type Item = U;
//     fn get_rdd_base(&self) -> Arc<dyn RddBase> {
//         Arc::new(self.clone()) as Arc<dyn RddBase>
//     }

//     fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
//         Arc::new(self.clone())
//     }

//     fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
//         Ok(Box::new(self.prev.iterator(split)?.map(self.f.clone())))
//     }
// }
