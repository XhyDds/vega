use std::fmt::Display;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::env;
use crate::rdd::Rdd;
use crate::scheduler::{Task, TaskBase, TaskContext};
use crate::serializable_traits::{AnyData, Data};
use crate::SerBox;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};

/**
 * 结构体：ResultTask
 * 描述：ResultTask是一个Task，它将计算结果作为一个RDD的一部分返回。
 * 成员：
 * task_id: usize，Task的ID
 * run_id: usize，Task的运行ID
 * stage_id: usize，Task所属的Stage的ID
 * pinned: bool，Task是否被固定
 * rdd: Arc<dyn Rdd<Item = T>>，Task所属的RDD
 * func: Arc<F>，Task的计算函数
 * partition: usize，Task所属的分区
 * locs: Vec<Ipv4Addr>，Task所在的位置，IP地址
 * output_id: usize，Task的输出ID
 * _marker: PhantomData<T>，泛型标记
 */
#[derive(Serialize, Deserialize)]
pub(crate) struct ResultTask<T: Data, U: Data, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pinned: bool,
    #[serde(with = "serde_traitobject")]
    pub rdd: Arc<dyn Rdd<Item = T>>,
    pub func: Arc<F>,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
    pub output_id: usize,
    _marker: PhantomData<T>,
}

/**
 * 方法：fmt
 * 描述：用于格式化输出
 */
impl<T: Data, U: Data, F> Display for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultTask({}, {})", self.stage_id, self.partition)
    }
}

/**
 * 方法：clone
 * 描述：复制一个ResultTask
 */
impl<T: Data, U: Data, F> ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    pub fn clone(&self) -> Self {
        ResultTask {
            task_id: self.task_id,
            run_id: self.run_id,
            stage_id: self.stage_id,
            pinned: self.rdd.is_pinned(),
            rdd: self.rdd.clone(),
            func: self.func.clone(),
            partition: self.partition,
            locs: self.locs.clone(),
            output_id: self.output_id,
            _marker: PhantomData,
        }
    }
}

/**
 * 方法：new
 * 描述：新建一个ResultTask
 */
impl<T: Data, U: Data, F> ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: Arc<F>,
        partition: usize,
        locs: Vec<Ipv4Addr>,
        output_id: usize,
    ) -> Self {
        ResultTask {
            task_id,
            run_id,
            stage_id,
            pinned: rdd.is_pinned(),
            rdd,
            func,
            partition,
            locs,
            output_id,
            _marker: PhantomData,
        }
    }
}

/**
 * 一些获取ResultTask信息的方法
 */
impl<T: Data, U: Data, F> TaskBase for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    fn get_run_id(&self) -> usize {
        self.run_id
    }

    fn get_stage_id(&self) -> usize {
        self.stage_id
    }

    fn get_task_id(&self) -> usize {
        self.task_id
    }

    fn is_pinned(&self) -> bool {
        self.pinned
    }

    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        self.locs.clone()
    }

    fn generation(&self) -> Option<i64> {
        Some(env::Env::get().map_output_tracker.get_generation())
    }
}

/**
 * 方法：run
 * 描述：运行ResultTask，先根据rdd和partition获取到对应的分区，然后调用func计算
 */
impl<T: Data, U: Data, F> Task for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    fn run(&self, id: usize) -> SerBox<dyn AnyData> {
        let split = self.rdd.splits()[self.partition].clone();
        let context = TaskContext::new(self.stage_id, self.partition, id);
        SerBox::new((self.func)((context, self.rdd.iterator(split).unwrap())))
            as SerBox<dyn AnyData>
    }
}
