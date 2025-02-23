use std::cmp::Ordering;
use std::net::Ipv4Addr;

use crate::scheduler::ResultTask;
use crate::serializable_traits::{AnyData, Data, SerFunc};
use crate::shuffle::ShuffleMapTask;
use crate::SerBox;
use downcast_rs::{impl_downcast, Downcast};
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};

/**
 * 结构体：TaskContext
 * 描述：TaskContext是Task的上下文，用于描述Task的一些信息
 * 成员：
 * stage_id: usize，Task所属的Stage的ID
 * split_id: usize，Task所属的分区的ID
 * attempt_id: usize，Task的尝试ID
 */
pub struct TaskContext {
    pub stage_id: usize,
    pub split_id: usize,
    pub attempt_id: usize,
}

impl TaskContext {
    pub fn new(stage_id: usize, split_id: usize, attempt_id: usize) -> Self {
        TaskContext {
            stage_id,
            split_id,
            attempt_id,
        }
    }
}

/**
 * 特性：TaskBase
 * 描述：Task的基类，包括一些获取信息的方法
 */
pub trait TaskBase: Downcast + Send + Sync {
    fn get_run_id(&self) -> usize;
    fn get_stage_id(&self) -> usize;
    fn get_task_id(&self) -> usize;
    fn is_pinned(&self) -> bool {
        false
    }
    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        Vec::new()
    }
    fn generation(&self) -> Option<i64> {
        None
    }
}
impl_downcast!(TaskBase);

/**
 * 比较方法：PartialOrd、PartialEq、Eq、Ord
 * 与其他的TaskBase进行比较，比较的是Task的ID，用于升序排序
 */
impl PartialOrd for dyn TaskBase {
    fn partial_cmp(&self, other: &dyn TaskBase) -> Option<Ordering> {
        Some(self.get_task_id().cmp(&other.get_task_id()))
    }
}

impl PartialEq for dyn TaskBase {
    fn eq(&self, other: &dyn TaskBase) -> bool {
        self.get_task_id() == other.get_task_id()
    }
}

impl Eq for dyn TaskBase {}

impl Ord for dyn TaskBase {
    fn cmp(&self, other: &dyn TaskBase) -> Ordering {
        self.get_task_id().cmp(&other.get_task_id())
    }
}

pub(crate) trait Task: TaskBase + Send + Sync + Downcast {
    fn run(&self, id: usize) -> SerBox<dyn AnyData>;
}

impl_downcast!(Task);

pub(crate) trait TaskBox: Task + Serialize + Deserialize + 'static + Downcast {}

impl<K> TaskBox for K where K: Task + Serialize + Deserialize + 'static {}

impl_downcast!(TaskBox);

#[derive(Serialize, Deserialize)]
pub(crate) enum TaskOption {
    #[serde(with = "serde_traitobject")]
    ResultTask(Box<dyn TaskBox>),
    #[serde(with = "serde_traitobject")]
    ShuffleMapTask(Box<dyn TaskBox>),
}

impl<T: Data, U: Data, F> From<ResultTask<T, U, F>> for TaskOption
where
    F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
{
    fn from(t: ResultTask<T, U, F>) -> Self {
        TaskOption::ResultTask(Box::new(t) as Box<dyn TaskBox>)
    }
}

impl From<ShuffleMapTask> for TaskOption {
    fn from(t: ShuffleMapTask) -> Self {
        TaskOption::ResultTask(Box::new(t))
    }
}

// can't run successfully,need to be fixed
// impl Clone for TaskOption {
//     fn clone(&self) -> Self {
//         match self {
//             TaskOption::ResultTask(tsk) => TaskOption::ResultTask(tsk.as_ref().clone_box()),
//             TaskOption::ShuffleMapTask(tsk) => TaskOption::ShuffleMapTask(*(tsk.clone())),
//         }
//     }
// }

#[derive(Serialize, Deserialize)]
pub(crate) enum TaskResult {
    ResultTask(SerBox<dyn AnyData>),
    ShuffleTask(SerBox<dyn AnyData>),
}

impl TaskOption {
    pub fn run(&self, id: usize) -> TaskResult {
        match self {
            TaskOption::ResultTask(tsk) => TaskResult::ResultTask(tsk.run(id)),
            TaskOption::ShuffleMapTask(tsk) => TaskResult::ShuffleTask(tsk.run(id)),
        }
    }

    pub fn get_task_id(&self) -> usize {
        match self {
            TaskOption::ResultTask(tsk) => tsk.get_task_id(),
            TaskOption::ShuffleMapTask(tsk) => tsk.get_task_id(),
        }
    }

    pub fn get_run_id(&self) -> usize {
        match self {
            TaskOption::ResultTask(tsk) => tsk.get_run_id(),
            TaskOption::ShuffleMapTask(tsk) => tsk.get_run_id(),
        }
    }

    pub fn get_stage_id(&self) -> usize {
        match self {
            TaskOption::ResultTask(tsk) => tsk.get_stage_id(),
            TaskOption::ShuffleMapTask(tsk) => tsk.get_stage_id(),
        }
    }
}
