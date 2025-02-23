use std::collections::{btree_set::BTreeSet, vec_deque::VecDeque, HashMap, HashSet};
use std::fmt::Debug;
use std::iter::FromIterator;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crate::dependency::ShuffleDependencyTrait;
use crate::error::{Error, NetworkError, Result};
use crate::map_output_tracker::MapOutputTracker;
use crate::partial::{ApproximateActionListener, ApproximateEvaluator, PartialResult};
use crate::rdd::{Rdd, RddBase};
use crate::scheduler::{
    listener::{JobEndListener, JobStartListener},
    CompletionEvent, EventQueue, FetchFailedVals, Job, JobListener, JobTracker, LiveListenerBus,
    NativeScheduler, NoOpListener, ResultTask, Stage, TaskBase, TaskContext, TaskOption,
    TaskResult, TastEndReason,
};
use crate::serializable_traits::{AnyData, Data, SerFunc};
use crate::serialized_data_capnp::serialized_data;
use crate::shuffle::ShuffleMapTask;
use crate::{env, monitor};
use capnp::message::ReaderOptions;
use capnp_futures::serialize as capnp_serialize;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio_util::compat::{Tokio02AsyncReadCompatExt, Tokio02AsyncWriteCompatExt};

const CAPNP_BUF_READ_OPTS: ReaderOptions = ReaderOptions {
    traversal_limit_in_words: std::u64::MAX,
    nesting_limit: 64,
};

// Just for now, creating an entire scheduler functions without dag scheduler trait.
// Later change it to extend from dag scheduler.
#[derive(Clone, Default)]
pub(crate) struct DistributedScheduler {
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    event_queues: EventQueue, //事件队列（元素为：{run_id,values(?)}，run_id由job_tracker提供）
    next_job_id: Arc<AtomicUsize>,
    next_run_id: Arc<AtomicUsize>,
    next_task_id: Arc<AtomicUsize>,
    next_stage_id: Arc<AtomicUsize>,
    stage_cache: Arc<DashMap<usize, Stage>>,
    shuffle_to_map_stage: Arc<DashMap<usize, Stage>>,
    cache_locs: Arc<DashMap<usize, Vec<Vec<Ipv4Addr>>>>,
    master: bool,
    framework_name: String,
    is_registered: bool,              // NOTE: check if it is necessary
    active_jobs: HashMap<usize, Job>, //实际没有使用
    active_job_queue: Vec<Job>,       //实际没有使用
    taskid_to_jobid: HashMap<String, usize>,
    taskid_to_slaveid: HashMap<String, String>,
    job_tasks: HashMap<usize, HashSet<String>>, //实际没有使用
    slaves_with_executors: HashSet<String>,
    server_uris: Arc<Mutex<VecDeque<SocketAddrV4>>>, //server_uri队列(task未pin时，循环取出地址分配
    port: u16,
    map_output_tracker: MapOutputTracker,
    // NOTE: fix proper locking mechanism
    scheduler_lock: Arc<Mutex<bool>>, // lock上之后只有出作用域才会释放
    live_listener_bus: LiveListenerBus,
}

impl DistributedScheduler {
    pub fn new(
        max_failures: usize,
        master: bool,
        servers: Option<Vec<SocketAddrV4>>,
        port: u16,
    ) -> Self {
        log::debug!(
            "starting distributed scheduler @ port {} (in master mode: {})",
            port,
            master,
        );
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus.start().unwrap();
        DistributedScheduler {
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
            framework_name: "vega".to_string(),
            is_registered: true, // NOTE: check if it is necessary
            active_jobs: HashMap::new(),
            active_job_queue: Vec::new(),
            taskid_to_jobid: HashMap::new(),
            taskid_to_slaveid: HashMap::new(),
            job_tasks: HashMap::new(),
            slaves_with_executors: HashSet::new(),
            server_uris: if let Some(servers) = servers {
                Arc::new(Mutex::new(VecDeque::from_iter(servers)))
            } else {
                Arc::new(Mutex::new(VecDeque::new()))
            },
            port,
            map_output_tracker: env::Env::get().map_output_tracker.clone(),
            scheduler_lock: Arc::new(Mutex::new(true)),
            live_listener_bus,
        }
    }

    /// Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
    /// as they arrive. Returns a partial result object from the evaluator.
    /// 在给定的RDD上运行approximate job，并把结果传给approximateevaluator
    /// 当结果到达时，返回一个来自evaluator的partial result对象
    /// 和local版本完全一样
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
                tokio::spawn(self.event_process_loop(false, jt.clone()));
                jt.listener.get_result().await
            })
        })
    }

    /// 和local版本完全一样
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
        env::Env::run_in_async_rt(|| -> Result<Vec<U>> {
            futures::executor::block_on(async move {
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
    /// 与local版本一样
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
            if let Some(result) = DistributedScheduler::local_execution(jt.clone())? {
                return Ok(result);
            }
        }

        self.event_queues.insert(jt.run_id, VecDeque::new());

        let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
        let mut fetch_failure_duration = Duration::new(0, 0);

        self.submit_stage(jt.final_stage.clone(), jt.clone())
            .await?;
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
                        log::error!("{}:fetch failed", evt.task.get_task_id());
                        self.on_event_failure(jt.clone(), failed_vals, evt.task.get_stage_id())
                            .await;
                        fetch_failure_duration = start.elapsed();
                    }
                    Error(error) => panic!("{}", error),
                    OtherFailure(msg) => panic!("{}", msg),
                }
            }
        }

        if !jt.failed.lock().await.is_empty()
            && fetch_failure_duration.as_millis() > self.resubmit_timeout
        {
            self.update_cache_locs().await?;
            for stage in jt.failed.lock().await.iter() {
                self.submit_stage(stage.clone(), jt.clone()).await?;
            }
            jt.failed.lock().await.clear();
        }

        self.event_queues.remove(&jt.run_id);
        Ok(results
            .into_iter()
            .map(|s| match s {
                Some(v) => v,
                None => panic!("some results still missing"),
            })
            .collect())
    }

    /// 获取结果
    /// 逻辑类似local的run_task
    /// 只是加上了传输的逻辑
    async fn receive_results<T: Data, U: Data, F, R>(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        receiver: R,
        task: TaskOption,
        target_port: u16,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        R: futures::AsyncRead + std::marker::Unpin,
    {
        //为task_ended的参数做准备
        let result: TaskResult = {
            let message = capnp_futures::serialize::read_message(receiver, CAPNP_BUF_READ_OPTS)
                .await
                .unwrap()
                .ok_or_else(|| NetworkError::NoMessageReceived)
                .unwrap();
            let task_data = message.get_root::<serialized_data::Reader>().unwrap();
            log::info!(
                "received task #{} result of {} bytes from executor @{}",
                task.get_task_id(),
                task_data.get_msg().unwrap().len(),
                target_port
            );
            bincode::deserialize(&task_data.get_msg().unwrap()).unwrap()
        };

        tokio::spawn(async {
            let _ = monitor::poster::post(String::from("1")).await;
        });

        match task {
            TaskOption::ResultTask(tsk) => {
                let result = match result {
                    TaskResult::ResultTask(r) => r,
                    _ => panic!("wrong result type"),
                };
                if let Ok(task_final) = tsk.downcast::<ResultTask<T, U, F>>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    DistributedScheduler::task_ended(
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        // Can break in future. But actually not needed for distributed scheduler since task runs on different processes.
                        // Currently using this because local scheduler needs it. It can be solved by refactoring tasks differently for local and distributed scheduler
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
                    DistributedScheduler::task_ended(
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        result.into_box(),
                    );
                }
            }
        };
    }

    async fn task_failed<T: Data, U: Data, F>(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: TaskOption,
        target_executor: SocketAddrV4,
        map_id: usize,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        let reason = TastEndReason::FetchFailed(FetchFailedVals {
            server_uri: target_executor.to_string(),
            shuffle_id: task.get_run_id(),
            map_id: map_id,
            reduce_id: 0, //用不到
        });
        log::debug!(
            "server_uri:{}\nshuffle_id:{}\nmap_id:{}\n",
            target_executor.to_string(),
            task.get_stage_id(),
            task.get_task_id()
        );
        let result = String::from("Error");
        match task {
            TaskOption::ResultTask(tsk) => {
                if let Ok(task_final) = tsk.downcast::<ResultTask<T, U, F>>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    DistributedScheduler::task_ended(
                        event_queues,
                        task_final,
                        reason,
                        // Can break in future. But actually not needed for distributed scheduler since task runs on different processes.
                        // Currently using this because local scheduler needs it. It can be solved by refactoring tasks differently for local and distributed scheduler
                        Box::new(result),
                    );
                }
            }
            TaskOption::ShuffleMapTask(tsk) => {
                if let Ok(task_final) = tsk.downcast::<ShuffleMapTask>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    DistributedScheduler::task_ended(
                        event_queues,
                        task_final,
                        reason,
                        Box::new(result),
                    );
                }
            }
        };
    }

    /// 将result加入到event_queues中
    /// 与local版本一样
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
impl NativeScheduler for DistributedScheduler {
    async fn submit_task_iter<T: Data, U: Data, F>(
        task: TaskOption,
        id_in_job: usize,
        target_executor: SocketAddrV4,
        socket_addrs: Arc<Mutex<VecDeque<SocketAddrV4>>>,
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        tokio::spawn(async move {
            let mut num_retries = 0;
            loop {
                match TcpStream::connect(&target_executor).await {
                    Ok(mut stream) => {
                        log::debug!("connected to exec @{}", target_executor.port());
                        let (reader, writer) = stream.split();
                        let reader = reader.compat();
                        let writer = writer.compat_write();
                        log::debug!("serialize task:exec @{}", target_executor.port());
                        let task_bytes = bincode::serialize(&task).unwrap();
                        log::debug!(
                            "sending task #{} of {} bytes to exec @{},",
                            task.get_task_id(),
                            task_bytes.len(),
                            target_executor.port(),
                        );

                        log::debug!("sending message to exec @{}", target_executor.port());

                        // TODO: 可考虑remove blocking call when possible
                        futures::executor::block_on(async {
                            // 发送端
                            let mut message = capnp::message::Builder::new_default();
                            let mut task_data = message.init_root::<serialized_data::Builder>();
                            task_data.set_msg(&task_bytes);
                            println!("task bytes len: {}", task_bytes.len());
                            capnp_serialize::write_message(writer, message)
                                .await
                                .map_err(Error::CapnpDeserialization)
                                .unwrap();
                        });

                        log::debug!("sent data to exec @{}", target_executor.port());
                        log::debug!(
                            "sent task {} to exec @{} ({})",
                            task.get_task_id(),
                            target_executor.port(),
                            target_executor.to_string()
                        );

                        // receive results back
                        // 接收到result
                        DistributedScheduler::receive_results::<T, U, F, _>(
                            event_queues,
                            reader,
                            task,
                            target_executor.port(),
                        )
                        .await;
                        break;
                    }
                    Err(_) => {
                        // 允许错误五次，每次等待之后重新发送请求
                        // 五次后不再尝试发送，将任务标记为failed，等待发送给其他executor
                        if num_retries > 5 {
                            //重新发送
                            log::error!("executor @{} can not response", target_executor.port());
                            let new_executor = socket_addrs.lock().pop_back().unwrap();
                            socket_addrs.lock().push_front(new_executor);
                            log::error!(
                                "Fault Tolerance: task_id:{}, old executor:{}, new executor:{}",
                                task.get_task_id(),
                                target_executor.to_string(),
                                new_executor.to_string()
                            );
                            let target_executor = new_executor;
                            DistributedScheduler::submit_task_iter::<T, U, F>(
                                task,
                                id_in_job,
                                target_executor,
                                socket_addrs,
                                event_queues,
                            )
                            .await;
                            break;
                        }
                        tokio::time::delay_for(Duration::from_millis(200)).await;
                        num_retries += 1;
                        continue;
                    }
                }
            }
        });
    }
    /// 提交task
    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        _id_in_job: usize,
        target_executor: SocketAddrV4,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        if !env::Configuration::get().is_driver {
            // 如果不是主机，则不需要调度
            return;
        }
        log::debug!("inside submit task");
        log::info!("submit task {}", task.get_task_id());
        tokio::spawn(async {
            let _ = monitor::poster::post(String::from("0")).await;
        });
        let event_queues_clone = self.event_queues.clone();
        let socket_addrs = self.server_uris.clone();
        tokio::spawn(async move {
            DistributedScheduler::submit_task_iter::<T, U, F>(
                task,
                _id_in_job,
                target_executor,
                socket_addrs,
                event_queues_clone,
            )
            .await;
        });
    }

    /// 获取下一个executor的地址
    fn next_executor_server(&self, task: &dyn TaskBase) -> SocketAddrV4 {
        if !task.is_pinned() {
            // pick the first available server
            // 没被pin住，则任意一个可行的就好
            // 从server_uris末尾取出一个，然后放到开头
            let socket_addrs = self.server_uris.lock().pop_back().unwrap();
            self.server_uris.lock().push_front(socket_addrs);
            socket_addrs
        } else {
            // seek and pick the selected host
            // 被pin住了，要找到对应的
            // 放到开头
            let servers = &mut *self.server_uris.lock();
            let location: Ipv4Addr = task.preferred_locations()[0];
            if let Some((pos, _)) = servers
                .iter()
                .enumerate()
                .find(|(_, e)| *e.ip() == location)
            {
                let target_host = servers.remove(pos).unwrap();
                servers.push_front(target_host);
                target_host
            } else {
                unreachable!()
            }
        }
    }

    /// 更新cache_locs
    /// 和local版本一样
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
    /// 把一个stage分配给一个shuffle
    /// 和local版本一样
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

    /// 获取missing的parent
    /// 和local版本一样
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

impl Drop for DistributedScheduler {
    fn drop(&mut self) {
        self.live_listener_bus.stop().unwrap();
    }
}
