use std::any::Any;
use std::collections::HashMap;
use std::error::Error;

use crate::scheduler::{Scheduler, TaskBase};
use crate::serializable_traits::AnyData;

/**
 * 结构体：FetchFailedVals
 * 描述：FetchFailedVals用于描述Fetch失败的信息
 * 成员：
 * server_uri: String，Fetch失败的Server所在的URI
 * shuffle_id: usize，Fetch失败的Shuffle的ID
 * map_id: usize，Fetch失败的Map的ID
 * reduce_id: usize，Fetch失败的Reduce的ID
 */
#[derive(Debug, Clone)]
pub struct FetchFailedVals {
    pub server_uri: String,
    pub shuffle_id: usize,
    pub map_id: usize,
    pub reduce_id: usize,
}

/**
 * 结构体：CompletionEvent
 * 描述：CompletionEvent用于描述Task完成的事件
 * 成员：
 * task: Box<dyn TaskBase>，Task的基础信息
 * reason: TastEndReason，Task完成的原因
 * result: Option<Box<dyn AnyData>>，Task的结果
 * accum_updates: HashMap<i64, Box<dyn Any + Send + Sync>>，Task的累加器更新
 */
// Send, Sync are required only because of local scheduler where threads are used.
// Since distributed scheduler runs tasks on different processes, such restriction is not required.
// Have to redesign this because serializing the Send, Sync traits is not such a good idea.
pub struct CompletionEvent {
    pub task: Box<dyn TaskBase>,
    pub reason: TastEndReason, // FIXME: 此处应改为TaskEndReason
    pub result: Option<Box<dyn AnyData>>,
    pub accum_updates: HashMap<i64, Box<dyn Any + Send + Sync>>,
}

pub enum TastEndReason {
    Success,
    FetchFailed(FetchFailedVals),
    Error(Box<dyn Error + Send + Sync>),
    OtherFailure(String),
}

pub trait DAGTask: TaskBase {
    fn get_run_id(&self) -> usize;
    fn get_stage_id(&self) -> usize;
    fn get_gen(&self) -> i64;
    fn generation(&self) -> Option<i64> {
        Some(self.get_gen())
    }
}

pub trait DAGScheduler: Scheduler {
    fn submit_tasks(&self, tasks: Vec<Box<dyn TaskBase>>, run_id: i64);
    fn task_ended(
        task: Box<dyn TaskBase>,
        reason: TastEndReason,
        result: Box<dyn Any>,
        accum_updates: HashMap<i64, Box<dyn Any>>,
    );
}
