use std::clone::Clone;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::option::Option;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crate::dependency::ShuffleDependencyTrait;
use crate::map_output_tracker::MapOutputTracker;
use crate::partial::{ApproximateActionListener, ApproximateEvaluator, PartialResult};
use crate::rdd::{Rdd, RddBase};
use crate::scheduler::{
    listener::{JobEndListener, JobStartListener},
    CompletionEvent, EventQueue, Job, JobListener, JobTracker, LiveListenerBus, NativeScheduler,
    NoOpListener, ResultTask, Stage, TaskBase, TaskContext, TaskOption, TaskResult, TastEndReason,
};
use crate::serializable_traits::{AnyData, Data, SerFunc};
use crate::shuffle::ShuffleMapTask;
use crate::{env, monitor, Result};
use dashmap::DashMap;
use parking_lot::Mutex;

/**
 * 结构体：LocalScheduler
 * 描述：单机版的调度器
 * 成员：
 * max_failures: usize, 最大失败次数
 * attempt_id: Arc<AtomicUsize>, 尝试id
 * resubmit_timeout: u128, 重新提交超时时间
 * poll_timeout: u64, 轮询超时时间
 * event_queues: EventQueue, 事件队列
 * stage_cache: Arc<DashMap<usize, Stage>>, stage缓存
 * shuffle_to_map_stage: Arc<DashMap<usize, Stage>>, shuffle到map阶段
 * cache_locs: Arc<DashMap<usize, Vec<Vec<Ipv4Addr>>>>, 缓存位置
 * master: bool, 是否是master
 * framework_name: String, 框架名称
 * is_registered: bool, 是否已注册
 * active_jobs: HashMap<usize, Job>, 活跃的job
 * active_job_queue: Vec<Job>, 活跃的job队列
 * taskid_to_jobid: HashMap<String, usize>, taskid到jobid的映射
 * taskid_to_slaveid: HashMap<String, String>, taskid到slaveid的映射
 * job_tasks: HashMap<usize, HashSet<String>>, job到task的映射
 * slaves_with_executors: HashSet<String>, 有执行器的slave
 * map_output_tracker: MapOutputTracker, map输出跟踪器
 * scheduler_lock: Arc<Mutex<()>>, 调度器锁
 * live_listener_bus: LiveListenerBus, 活跃的监听总线
 */
#[derive(Clone, Default)]
pub(crate) struct LocalScheduler {
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    event_queues: EventQueue,
    pub(crate) next_job_id: Arc<AtomicUsize>,
    next_run_id: Arc<AtomicUsize>,
    next_task_id: Arc<AtomicUsize>,
    next_stage_id: Arc<AtomicUsize>,
    stage_cache: Arc<DashMap<usize, Stage>>,
    shuffle_to_map_stage: Arc<DashMap<usize, Stage>>,
    cache_locs: Arc<DashMap<usize, Vec<Vec<Ipv4Addr>>>>,
    master: bool,
    framework_name: String,
    is_registered: bool, // NOTE: check if it is necessary
    active_jobs: HashMap<usize, Job>,
    active_job_queue: Vec<Job>,
    taskid_to_jobid: HashMap<String, usize>,
    taskid_to_slaveid: HashMap<String, String>,
    job_tasks: HashMap<usize, HashSet<String>>,
    slaves_with_executors: HashSet<String>,
    map_output_tracker: MapOutputTracker,
    // NOTE: fix proper locking mechanism
    scheduler_lock: Arc<Mutex<()>>,
    live_listener_bus: LiveListenerBus,
}

impl LocalScheduler {
    pub fn new(max_failures: usize, master: bool) -> Self {
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus.start().unwrap();
        LocalScheduler {
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            resubmit_timeout: 2000,
            poll_timeout: 50,
            event_queues: Arc::new(DashMap::new()),
            next_job_id: Arc::new(AtomicUsize::new(0)),
            next_run_id: Arc::new(AtomicUsize::new(0)),
            next_task_id: Arc::new(AtomicUsize::new(0)),
            next_stage_id: Arc::new(AtomicUsize::new(0)),
            stage_cache: Arc::new(DashMap::new()),
            shuffle_to_map_stage: Arc::new(DashMap::new()),
            cache_locs: Arc::new(DashMap::new()),
            master,
            framework_name: "spark".to_string(),
            is_registered: true, // NOTE: check if it is necessary
            active_jobs: HashMap::new(),
            active_job_queue: Vec::new(),
            taskid_to_jobid: HashMap::new(),
            taskid_to_slaveid: HashMap::new(),
            job_tasks: HashMap::new(),
            slaves_with_executors: HashSet::new(),
            map_output_tracker: env::Env::get().map_output_tracker.clone(),
            scheduler_lock: Arc::new(Mutex::new(())),
            live_listener_bus,
        }
    }

    /// Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
    /// as they arrive. Returns a partial result object from the evaluator.
    /// 在给定的RDD上运行approximate job，并把结果传给approximateevaluator
    /// 当结果到达时，返回一个来自evaluator的partial result对象
    pub fn run_approximate_job<T: Data, U: Data, R, F, E>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: Duration,
    ) -> Result<PartialResult<R>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        // acquiring lock so that only one job can run at same time this lock is just
        // a temporary patch for preventing multiple jobs to update cache locks which affects
        // construction of dag task graph. dag task graph construction needs to be altered
        let selfc = self.clone();
        let _lock = selfc.scheduler_lock.lock();
        env::Env::run_in_async_rt(|| -> Result<PartialResult<R>> {
            futures::executor::block_on(async move {
                let partitions: Vec<_> = (0..final_rdd.number_of_splits()).collect();
                let listener = ApproximateActionListener::new(evaluator, timeout, partitions.len());
                let jt = JobTracker::from_scheduler(
                    &*self,
                    func,
                    final_rdd.clone(),
                    partitions,
                    listener,
                )
                .await?;
                if final_rdd.number_of_splits() == 0 {
                    // Return immediately if the job is running 0 tasks
                    // 如果job没有对应task，直接传送结果
                    let time = Instant::now();
                    self.live_listener_bus.post(Box::new(JobStartListener {
                        job_id: jt.run_id,
                        time,
                        stage_infos: vec![],
                    }));
                    self.live_listener_bus.post(Box::new(JobEndListener {
                        job_id: jt.run_id,
                        time,
                        job_result: true,
                    }));
                    return Ok(PartialResult::new(
                        jt.listener.evaluator.lock().await.current_result(),
                        true,
                    ));
                }
                // 有对应的task，将对应的JobTracker进行处理，返回结果
                tokio::spawn(self.event_process_loop(false, jt.clone()));
                jt.listener.get_result().await
            })
        })
    }

    /// 处理方式类似于run_approximate_job
    /// 在给定的RDD上运行job，并把结果传给evaluator
    /// 当结果到达时，返回一个来自evaluator的partial result对象
    pub fn run_job<T: Data, U: Data, F>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        // acquiring lock so that only one job can run at same time this lock is just
        // a temporary patch for preventing multiple jobs to update cache locks which affects
        // construction of dag task graph. dag task graph construction needs to be altered
        let selfc = self.clone();
        let _lock = selfc.scheduler_lock.lock();
        log::debug!("get lock for scheduler");
        //异步执行
        env::Env::run_in_async_rt(|| -> Result<Vec<U>> {
            //传入的闭包非异步，用block_on包装
            futures::executor::block_on(async move {
                //由fun和rdd、分区生成一个jobtracker
                let jt = JobTracker::from_scheduler(
                    &*self,
                    func,
                    final_rdd.clone(),
                    partitions,
                    NoOpListener,
                )
                .await?;
                self.event_process_loop(allow_local, jt).await
            })
        })
    }

    /// Start the event processing loop for a given job.
    /// 循环处理给定的job (由jobtracker表示)
    async fn event_process_loop<T: Data, U: Data, F, L>(
        self: Arc<Self>,
        allow_local: bool,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        // TODO: update cache
        if allow_local {
            // 允许在本地运行，使用local_execution
            if let Some(result) = LocalScheduler::local_execution(jt.clone())? {
                return Ok(result);
            }
        }

        // 不允许在本地运行，将job提交到event_queues中
        self.event_queues.insert(jt.run_id, VecDeque::new());

        let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
        let mut fetch_failure_duration = Duration::new(0, 0);
        self.submit_stage(jt.final_stage.clone(), jt.clone())
            .await?; //*对于蒙特卡洛计算Pi，2/3的时间用于这一条语句
        log::debug!(
            "pending stages and tasks: {:?}",
            jt.pending_tasks
                .lock()
                .await
                .iter()
                .map(|(k, v)| (k.id, v.iter().map(|x| x.get_task_id()).collect::<Vec<_>>()))
                .collect::<Vec<_>>()
        );

        let mut num_finished = 0;
        while num_finished != jt.num_output_parts {
            let event_option = self.wait_for_event(jt.run_id, self.poll_timeout);
            let start = Instant::now();

            if let Some(evt) = event_option {
                log::debug!("event starting");
                let stage = self
                    .stage_cache
                    .get(&evt.task.get_stage_id())
                    .unwrap()
                    .clone();
                log::debug!(
                    "removing stage #{} task from pending task #{}",
                    stage.id,
                    evt.task.get_task_id()
                );
                jt.pending_tasks
                    .lock()
                    .await
                    .get_mut(&stage)
                    .unwrap()
                    .remove(&evt.task);
                use super::dag_scheduler::TastEndReason::*;
                match evt.reason {
                    Success => {
                        self.on_event_success(evt, &mut results, &mut num_finished, jt.clone())
                            .await?;
                    }
                    FetchFailed(failed_vals) => {
                        self.on_event_failure(jt.clone(), failed_vals, evt.task.get_stage_id())
                            .await;
                        fetch_failure_duration = start.elapsed();
                    }
                    Error(error) => panic!("{}", error),
                    OtherFailure(msg) => panic!("{}", msg),
                }
            }
        }
        //*对于蒙特卡洛计算Pi，1/3时间用于这个while循环体

        // 如果有失败的task，且失败时间超过resubmit_timeout，重新提交
        // NOTE: 是否需要再走一遍while的流程？
        if !jt.failed.lock().await.is_empty()
            && fetch_failure_duration.as_millis() > self.resubmit_timeout
        {
            self.update_cache_locs().await?;
            for stage in jt.failed.lock().await.iter() {
                self.submit_stage(stage.clone(), jt.clone()).await?;
            }
            jt.failed.lock().await.clear();
        }

        // 完成之后从event_queues中去除
        self.event_queues.remove(&jt.run_id);
        Ok(results
            .into_iter()
            .map(|s| match s {
                Some(v) => v,
                None => panic!("some results still missing"),
            })
            .collect())
    }

    /// 运行task
    /// 如果task和attempt_id对应的result相对应，则返回正确结果，否则panic
    fn run_task<T: Data, U: Data, F>(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: Vec<u8>,
        _id_in_job: usize,
        attempt_id: usize,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        log::info!("submit task {}", attempt_id);
        let des_task: TaskOption = bincode::deserialize(&task).unwrap(); //*反序列化task花了Pi计算时间的一半
        let result = des_task.run(attempt_id);
        log::info!("received task {} result", attempt_id);
        tokio::spawn(async {
            let _ = monitor::poster::post(String::from("1")).await;
        });
        match des_task {
            TaskOption::ResultTask(tsk) => {
                let result = match result {
                    TaskResult::ResultTask(r) => r,
                    _ => panic!("wrong result type"),
                };
                if let Ok(task_final) = tsk.downcast::<ResultTask<T, U, F>>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    LocalScheduler::task_ended(
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        result.into_box(),
                    );
                }
            }
            TaskOption::ShuffleMapTask(tsk) => {
                let result = match result {
                    TaskResult::ShuffleTask(r) => r,
                    _ => panic!("wrong result type"),
                };
                if let Ok(task_final) = tsk.downcast::<ShuffleMapTask>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    LocalScheduler::task_ended(
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        result.into_box(),
                    );
                }
            }
        };
    }

    /// 将result加入到event_queues中
    fn task_ended(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: Box<dyn TaskBase>,
        reason: TastEndReason,
        result: Box<dyn AnyData>,
        // TODO: accumvalues needs to be done
    ) {
        let result = Some(result);
        if let Some(mut queue) = event_queues.get_mut(&(task.get_run_id())) {
            queue.push_back(CompletionEvent {
                task,
                reason,
                result,
                accum_updates: HashMap::new(),
            });
        } else {
            log::debug!("ignoring completion event for DAG Job");
        }
    }
}

#[async_trait::async_trait]
impl NativeScheduler for LocalScheduler {
    /// Every single task is run in the local thread pool
    async fn submit_task_iter<T: Data, U: Data, F>(
        _task: TaskOption,
        _id_in_job: usize,
        _server_address: SocketAddrV4,
        _socket_addrs: Arc<Mutex<VecDeque<SocketAddrV4>>>,
        _event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
    }
    /// 提交task，在本地运行
    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        id_in_job: usize,
        _server_address: SocketAddrV4,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        log::debug!("inside submit task");
        log::error!("prepared task {}", self.attempt_id.load(Ordering::SeqCst));
        tokio::spawn(async {
            let _ = monitor::poster::post(String::from("0")).await;
        });
        let my_attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let event_queues = self.event_queues.clone();
        let task = bincode::serialize(&task).unwrap();

        tokio::task::spawn_blocking(move || {
            LocalScheduler::run_task::<T, U, F>(event_queues, task, id_in_job, my_attempt_id)
        });
    }

    /// 获取下一个executor的地址
    /// 对于local来说就是LOCALHOST 127.0.0.1:0
    fn next_executor_server(&self, _rdd: &dyn TaskBase) -> SocketAddrV4 {
        // Just point to the localhost
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)
    }

    /// 更新cache_locs
    /// 清空原有的，加入cache_tracker里的
    async fn update_cache_locs(&self) -> Result<()> {
        self.cache_locs.clear();
        env::Env::get()
            .cache_tracker
            .get_location_snapshot()
            .await?
            .into_iter()
            .for_each(|(k, v)| {
                self.cache_locs.insert(k, v);
            });
        Ok(())
    }

    /// 由shuffle获取shuffle_map_stage
    /// 如果可以在shuffle_to_map_stage里面找到，则返回
    /// 否则创建新的stage
    async fn get_shuffle_map_stage(&self, shuf: Arc<dyn ShuffleDependencyTrait>) -> Result<Stage> {
        log::debug!("getting shuffle map stage");
        let stage = self.shuffle_to_map_stage.get(&shuf.get_shuffle_id());
        match stage {
            Some(stage) => Ok(stage.clone()),
            None => {
                log::debug!("started creating shuffle map stage before");
                let stage = self
                    .new_stage(shuf.get_rdd_base(), Some(shuf.clone()))
                    .await?;
                self.shuffle_to_map_stage
                    .insert(shuf.get_shuffle_id(), stage.clone());
                log::debug!("finished inserting newly created shuffle stage");
                Ok(stage)
            }
        }
    }

    /// 从stage获取missing的parent
    /// NOTE: 待确认
    async fn get_missing_parent_stages(&'_ self, stage: Stage) -> Result<Vec<Stage>> {
        log::debug!("getting missing parent stages");
        let mut missing: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: BTreeSet<Arc<dyn RddBase>> = BTreeSet::new();
        self.visit_for_missing_parent_stages(&mut missing, &mut visited, stage.get_rdd())
            .await?;
        Ok(missing.into_iter().collect())
    }

    impl_common_scheduler_funcs!();
}

impl Drop for LocalScheduler {
    fn drop(&mut self) {
        self.live_listener_bus.stop().unwrap();
    }
}
