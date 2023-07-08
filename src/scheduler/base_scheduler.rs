use std::collections::{BTreeSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::dependency::{Dependency, ShuffleDependencyTrait};
use crate::env;
use crate::error::{Error, Result};
use crate::rdd::RddBase;
use crate::scheduler::{
    CompletionEvent, FetchFailedVals, JobListener, JobTracker, ResultTask, Stage, TaskBase,
    TaskContext, TaskOption,
};
use crate::serializable_traits::{Data, SerFunc};
use crate::shuffle::ShuffleMapTask;
use dashmap::DashMap;

use parking_lot::Mutex;

pub(crate) type EventQueue = Arc<DashMap<usize, VecDeque<CompletionEvent>>>;

/// Functionality of the library built-in schedulers
#[async_trait::async_trait]
pub(crate) trait NativeScheduler: Send + Sync {
    /// Fast path for execution. Runs the DD in the driver main thread if possible.
    /// 在本地运行，若最后的stage无父stage，且有单个输出分区，则在driver主线程中运行
    fn local_execution<T: Data, U: Data, F, L>(
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> Result<Option<Vec<U>>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        if jt.final_stage.parents.is_empty() && (jt.num_output_parts == 1) {
            let split = (jt.final_rdd.splits()[jt.output_parts[0]]).clone();
            let task_context = TaskContext::new(jt.final_stage.id, jt.output_parts[0], 0);
            Ok(Some(vec![(&jt.func)((
                task_context,
                jt.final_rdd.iterator(split)?,
            ))]))
        } else {
            Ok(None)
        }
    }

    async fn new_stage(
        &self,
        rdd_base: Arc<dyn RddBase>,
        shuffle_dependency: Option<Arc<dyn ShuffleDependencyTrait>>,
    ) -> Result<Stage> {
        log::debug!("creating new stage");
        // 注册rdd到cache_tracker
        env::Env::get()
            .cache_tracker
            .register_rdd(rdd_base.get_rdd_id(), rdd_base.number_of_splits())
            .await?;
        // 注册shuffle
        if let Some(dep) = shuffle_dependency.clone() {
            log::debug!("shuffle dependency exists, registering to map output tracker");
            self.register_shuffle(dep.get_shuffle_id(), rdd_base.number_of_splits());
            log::debug!("new stage tracker after");
        }
        let id = self.get_next_stage_id();
        log::debug!("new stage id #{}", id);
        let stage = Stage::new(
            id,
            rdd_base.clone(),
            shuffle_dependency,
            self.get_parent_stages(rdd_base).await?,
        );
        log::debug!("stage is built");
        self.insert_into_stage_cache(id, stage.clone());
        log::debug!("returning new stage #{}", id);
        Ok(stage)
    }

    /// 递归函数
    /// visited中没有rdd的时候需要将rdd加入到visited里面，否则不进行任何操作
    async fn visit_for_missing_parent_stages<'s, 'a: 's>(
        &'s self,
        missing: &'a mut BTreeSet<Stage>,
        visited: &'a mut BTreeSet<Arc<dyn RddBase>>,
        rdd: Arc<dyn RddBase>,
    ) -> Result<()> {
        log::debug!(
            "missing stages: {:?}",
            missing.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        log::debug!(
            "visited stages: {:?}",
            visited.iter().map(|x| x.get_rdd_id()).collect::<Vec<_>>()
        );
        if !visited.contains(&rdd) {
            visited.insert(rdd.clone());
            // TODO: CacheTracker register
            for _ in 0..rdd.number_of_splits() {
                // 仅当rdd找不到loc时候进行之后的操作
                let locs = self.get_cache_locs(rdd.clone());
                log::debug!("cache locs: {:?}", locs);
                if locs == None {
                    for dep in rdd.get_dependencies() {
                        log::debug!("for dep in missing stages ");
                        match dep {
                            // 此处处理
                            Dependency::ShuffleDependency(shuf_dep) => {
                                let stage = self.get_shuffle_map_stage(shuf_dep.clone()).await?;
                                log::debug!("shuffle stage #{} in missing stages", stage.id);
                                if !stage.is_available() {
                                    log::debug!(
                                        "inserting shuffle stage #{} in missing stages",
                                        stage.id
                                    );
                                    missing.insert(stage);
                                }
                            }
                            // 此处递归
                            Dependency::NarrowDependency(nar_dep) => {
                                log::debug!("narrow stage in missing stages");
                                self.visit_for_missing_parent_stages(
                                    missing,
                                    visited,
                                    nar_dep.get_rdd_base(),
                                )
                                .await?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// 递归函数
    /// visited中没有rdd的时候将rdd加入到parents里面，否则不进行任何操作
    async fn visit_for_parent_stages<'s, 'a: 's>(
        &'s self,
        parents: &'a mut BTreeSet<Stage>,
        visited: &'a mut BTreeSet<Arc<dyn RddBase>>,
        rdd: Arc<dyn RddBase>,
    ) -> Result<()> {
        log::debug!(
            "parent stages: {:?}",
            parents.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        log::debug!(
            "visited stages: {:?}",
            visited.iter().map(|x| x.get_rdd_id()).collect::<Vec<_>>()
        );
        if !visited.contains(&rdd) {
            // 仅在visited中没有rdd的时候进行之后的操作
            visited.insert(rdd.clone());
            // 将rdd注册到cache_tracker
            env::Env::get()
                .cache_tracker
                .register_rdd(rdd.get_rdd_id(), rdd.number_of_splits())
                .await?;
            for dep in rdd.get_dependencies() {
                match dep {
                    Dependency::ShuffleDependency(shuf_dep) => {
                        // shuffle依赖就插入到parents中
                        parents.insert(self.get_shuffle_map_stage(shuf_dep.clone()).await?);
                    }
                    Dependency::NarrowDependency(nar_dep) => {
                        // 如果是窄依赖就递归
                        self.visit_for_parent_stages(parents, visited, nar_dep.get_rdd_base())
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// 利用visit_for_parent_stages获取rdd的parents
    async fn get_parent_stages(&self, rdd: Arc<dyn RddBase>) -> Result<Vec<Stage>> {
        log::debug!("inside get parent stages");
        let mut parents: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: BTreeSet<Arc<dyn RddBase>> = BTreeSet::new();
        self.visit_for_parent_stages(&mut parents, &mut visited, rdd.clone())
            .await?;
        log::debug!(
            "parent stages: {:?}",
            parents.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        Ok(parents.into_iter().collect())
    }

    /// 失败时的处理
    async fn on_event_failure<T: Data, U: Data, F, L>(
        &self,
        jt: Arc<JobTracker<F, U, T, L>>,
        failed_vals: FetchFailedVals,
        stage_id: usize,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        let FetchFailedVals {
            server_uri,
            shuffle_id,
            map_id,
            ..
        } = failed_vals;
        // TODO: mapoutput tracker needs to be finished for this
        // let failed_stage = self.id_to_stage.lock().get(&stage_id).?.clone();
        let failed_stage = self.fetch_from_stage_cache(stage_id);

        println!("failed stage: {:?}", failed_stage.output_locs);
        // let shuffle = failed_stage
        //     .shuffle_dependency
        //     .clone()
        //     .ok_or_else(|| Error::Other)
        //     .expect("shuffle dependency not found");
        // let shuffle_id = shuffle.get_shuffle_id();

        // 从running中移除失败的stage，并加入到failed中
        jt.running.lock().await.remove(&failed_stage);
        jt.failed.lock().await.insert(failed_stage);
        self.remove_output_loc_from_stage(shuffle_id, map_id, &server_uri);
        self.unregister_map_output(shuffle_id, map_id, server_uri);
        jt.failed
            .lock()
            .await
            .insert(self.fetch_from_shuffle_to_cache(shuffle_id));
    }

    /// 成功时的处理
    async fn on_event_success<T: Data, U: Data, F, L>(
        &self,
        mut completed_event: CompletionEvent,
        results: &mut Vec<Option<U>>,
        num_finished: &mut usize,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> Result<()>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        // FIXME: logging
        // TODO: add to Accumulator

        let result_type = completed_event
            .task
            .downcast_ref::<ResultTask<T, U, F>>()
            .is_some();
        if result_type {
            // 如果完成的event的task是resultTask类型，则将result存入results，且将jt标志为已完成，num_finished增加1
            if let Ok(rt) = completed_event.task.downcast::<ResultTask<T, U, F>>() {
                let any_result = completed_event.result.take().ok_or_else(|| Error::Other)?;
                jt.listener
                    .task_succeeded(rt.output_id, &*any_result)
                    .await?;
                let result = any_result
                    .as_any()
                    .downcast_ref::<U>()
                    .ok_or_else(|| {
                        Error::DowncastFailure("generic type U in scheduler on success")
                    })?
                    .clone();
                results[rt.output_id] = Some(result);
                jt.finished.lock().await[rt.output_id] = true;
                *num_finished += 1;
            }
        } else if let Ok(smt) = completed_event.task.downcast::<ShuffleMapTask>() {
            // 此处为非resultTask类型，即为shuffleMapTask类型
            let shuffle_server_uri = completed_event
                .result
                .take()
                .ok_or_else(|| Error::Other)?
                .as_any()
                .downcast_ref::<String>()
                .ok_or_else(|| crate::Error::DowncastFailure("String"))?
                .clone();
            log::debug!(
                "completed shuffle task server uri: {:?}",
                shuffle_server_uri
            );
            self.add_output_loc_to_stage(smt.stage_id, smt.partition, shuffle_server_uri);

            // 从缓存获取stage
            let stage = self.fetch_from_stage_cache(smt.stage_id);
            // 一些log
            log::debug!(
                "pending stages: {:?}",
                jt.pending_tasks
                    .lock()
                    .await
                    .iter()
                    .map(|(x, y)| (x.id, y.iter().map(|k| k.get_task_id()).collect::<Vec<_>>()))
                    .collect::<Vec<_>>()
            );
            log::debug!(
                "pending tasks: {:?}",
                jt.pending_tasks
                    .lock()
                    .await
                    .get(&stage)
                    .ok_or_else(|| Error::Other)?
                    .iter()
                    .map(|x| x.get_task_id())
                    .collect::<Vec<_>>()
            );
            log::debug!(
                "running stages: {:?}",
                jt.running
                    .lock()
                    .await
                    .iter()
                    .map(|x| x.id)
                    .collect::<Vec<_>>()
            );
            log::debug!(
                "waiting stages: {:?}",
                jt.waiting
                    .lock()
                    .await
                    .iter()
                    .map(|x| x.id)
                    .collect::<Vec<_>>()
            );

            if jt.running.lock().await.contains(&stage)
                && jt
                    .pending_tasks
                    .lock()
                    .await
                    .get(&stage)
                    .ok_or_else(|| Error::Other)?
                    .is_empty()
            {
                // 如果stage在running中，且pending_tasks中的stage为空
                log::debug!("started registering map outputs");
                // FIXME: logging
                // 将stage移除running
                jt.running.lock().await.remove(&stage);
                if let Some(dep) = stage.shuffle_dependency {
                    // 如果stage有shuffle依赖，则将stage的output_locs中的第一个元素取出，作为locs
                    log::debug!(
                        "stage output locs before register mapoutput tracker: {:?}",
                        stage.output_locs
                    );
                    let locs = stage
                        .output_locs
                        .iter()
                        .map(|x| x.get(0).map(|s| s.to_owned()))
                        .collect();
                    log::debug!("locs for shuffle id #{}: {:?}", dep.get_shuffle_id(), locs);
                    // 将locs注册到mapOutputTracker中
                    self.register_map_outputs(dep.get_shuffle_id(), locs);
                    log::debug!("finished registering map outputs");
                }
                // TODO: Cache
                // 更新缓存
                self.update_cache_locs().await?;

                // 从waiting中的stage获取新的runnable
                let mut newly_runnable = Vec::new();
                let waiting_stages: Vec<_> = jt.waiting.lock().await.iter().cloned().collect();
                for stage in waiting_stages {
                    let missing_stages = self.get_missing_parent_stages(stage.clone()).await?;
                    log::debug!(
                        "waiting stage parent stages for stage #{} are: {:?}",
                        stage.id,
                        missing_stages.iter().map(|x| x.id).collect::<Vec<_>>()
                    );
                    if missing_stages.iter().next().is_none() {
                        // 如果stage的parent stages都已经完成，则将stage加入newly_runnable
                        newly_runnable.push(stage.clone())
                    }
                }
                for stage in &newly_runnable {
                    jt.waiting.lock().await.remove(stage);
                }
                for stage in &newly_runnable {
                    jt.running.lock().await.insert(stage.clone());
                }
                for stage in newly_runnable {
                    self.submit_missing_tasks(stage, jt.clone()).await?;
                }
            }
        }

        Ok(())
    }

    /// 递归函数
    /// 提交stage
    /// 如果stage不在waiting和running中，将stage加入到waiting中
    async fn submit_stage<T: Data, U: Data, F, L>(
        &self,
        stage: Stage,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> Result<()>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        log::debug!("submitting stage #{}", stage.id);
        if !jt.waiting.lock().await.contains(&stage) && !jt.running.lock().await.contains(&stage) {
            let missing = self.get_missing_parent_stages(stage.clone()).await?;
            log::debug!(
                "while submitting stage #{}, missing stages: {:?}",
                stage.id,
                missing.iter().map(|x| x.id).collect::<Vec<_>>()
            );
            if missing.is_empty() {
                // 没有缺失，直接提交
                self.submit_missing_tasks(stage.clone(), jt.clone()).await?; //*测试Pi时，本函数所有开销在此处
                jt.running.lock().await.insert(stage);
            } else {
                // 有缺失，将parent提交之后再提交
                for parent in missing {
                    // 递归
                    self.submit_stage(parent, jt.clone()).await?;
                }
                jt.waiting.lock().await.insert(stage);
            }
        }
        Ok(())
    }

    /// 提交没有parent的task
    async fn submit_missing_tasks<T: Data, U: Data, F, L>(
        &self,
        stage: Stage,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> Result<()>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        let mut pending_tasks = jt.pending_tasks.lock().await;
        // 从pending_tasks中获取stage对应的值，如果为空，则插入空的BTreeSet
        let my_pending = pending_tasks
            .entry(stage.clone())
            .or_insert_with(BTreeSet::new);
        if stage == jt.final_stage {
            // 如果是final stage
            log::debug!("final stage #{}", stage.id);
            for (id_in_job, (id, part)) in jt
                .output_parts
                .iter()
                .enumerate()
                .take(jt.num_output_parts)
                .enumerate()
            {
                // 生成ResultTask并提交
                let locs = self.get_preferred_locs(jt.final_rdd.get_rdd_base(), *part);
                let result_task = ResultTask::new(
                    self.get_next_task_id(),
                    jt.run_id,
                    jt.final_stage.id,
                    jt.final_rdd.clone(),
                    jt.func.clone(),
                    *part,
                    locs,
                    id,
                );
                let task = Box::new(result_task.clone()) as Box<dyn TaskBase>;
                let executor = self.next_executor_server(&*task);
                my_pending.insert(task);
                self.submit_task::<T, U, F>(
                    TaskOption::ResultTask(Box::new(result_task)),
                    id_in_job,
                    executor,
                );
            }
        } else {
            // 非finalstage
            for p in 0..stage.num_partitions {
                log::debug!("shuffle stage #{}", stage.id);
                if stage.output_locs[p].is_empty() {
                    // 仅在对应输出位置为空时，才创建shuffleMapTask并提交
                    let locs = self.get_preferred_locs(stage.get_rdd(), p);
                    log::debug!("creating task for stage #{} partition #{}", stage.id, p);
                    let shuffle_map_task = ShuffleMapTask::new(
                        self.get_next_task_id(),
                        jt.run_id,
                        stage.id,
                        stage.rdd.clone(),
                        stage
                            .shuffle_dependency
                            .clone()
                            .ok_or_else(|| Error::Other)?,
                        p,
                        locs,
                    );
                    log::debug!(
                        "creating task for stage #{}, partition #{} and shuffle id #{}",
                        stage.id,
                        p,
                        shuffle_map_task.dep.get_shuffle_id()
                    );
                    let task = Box::new(shuffle_map_task.clone()) as Box<dyn TaskBase>;
                    let executor = self.next_executor_server(&*task);
                    my_pending.insert(task);
                    self.submit_task::<T, U, F>(
                        TaskOption::ShuffleMapTask(Box::new(shuffle_map_task)),
                        p,
                        executor,
                    );
                }
            }
        }
        Ok(())
    }

    /// 等待事件
    fn wait_for_event(&self, run_id: usize, timeout: u64) -> Option<CompletionEvent> {
        // TODO: 可考虑make use of async to wait for events
        let end = Instant::now() + Duration::from_millis(timeout);
        // 直到get_event_queue里有run_id对应的事件时结束循环
        while self.get_event_queue().get(&run_id)?.is_empty() {
            if Instant::now() > end {
                // 超时，没等到
                return None;
            } else {
                thread::sleep(end - Instant::now());
            }
        }
        // 等到了
        self.get_event_queue().get_mut(&run_id)?.pop_front()
    }

    // NOTE: 这里无标注的函数通过下方的macro_rules!宏实现

    async fn submit_task_iter<T: Data, U: Data, F>(
        task: TaskOption,
        id_in_job: usize,
        target_executor: SocketAddrV4,
        socket_addrs: Arc<Mutex<VecDeque<SocketAddrV4>>>,
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U;
    /// 在distributed_scheduler和local_scheduler中实现
    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        id_in_job: usize,
        target_executor: SocketAddrV4,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U;

    // mutators:
    fn add_output_loc_to_stage(&self, stage_id: usize, partition: usize, host: String);
    fn insert_into_stage_cache(&self, id: usize, stage: Stage);
    // refreshes cache locations
    fn register_shuffle(&self, shuffle_id: usize, num_maps: usize);
    fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>);
    fn remove_output_loc_from_stage(&self, shuffle_id: usize, map_id: usize, server_uri: &str);
    /// 在distributed_scheduler和local_scheduler中实现
    async fn update_cache_locs(&self) -> Result<()>;
    fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String);

    // getters:
    fn fetch_from_stage_cache(&self, id: usize) -> Stage;
    fn fetch_from_shuffle_to_cache(&self, id: usize) -> Stage;
    fn get_cache_locs(&self, rdd: Arc<dyn RddBase>) -> Option<Vec<Vec<Ipv4Addr>>>;
    fn get_event_queue(&self) -> &Arc<DashMap<usize, VecDeque<CompletionEvent>>>;
    /// 在distributed_scheduler和local_scheduler中实现
    async fn get_missing_parent_stages<'a>(&'a self, stage: Stage) -> Result<Vec<Stage>>;
    fn get_next_job_id(&self) -> usize;
    fn get_next_stage_id(&self) -> usize;
    fn get_next_task_id(&self) -> usize;
    /// 在distributed_scheduler和local_scheduler中实现
    fn next_executor_server(&self, rdd: &dyn TaskBase) -> SocketAddrV4;

    /// 获取比较适合的位置
    fn get_preferred_locs(&self, rdd: Arc<dyn RddBase>, partition: usize) -> Vec<Ipv4Addr> {
        // TODO: have to implement this completely
        if let Some(cached) = self.get_cache_locs(rdd.clone()) {
            // 如果有缓存，直接返回
            if let Some(cached) = cached.get(partition) {
                return cached.clone();
            }
        }
        let rdd_prefs = rdd.preferred_locations(rdd.splits()[partition].clone());
        if !rdd.is_pinned() {
            // rdd没有被pin住
            if !rdd_prefs.is_empty() {
                return rdd_prefs;
            }
            // rdd_prefs为空，尝试从依赖中获取
            for dep in rdd.get_dependencies().iter() {
                if let Dependency::NarrowDependency(nar_dep) = dep {
                    for in_part in nar_dep.get_parents(partition) {
                        let locs = self.get_preferred_locs(nar_dep.get_rdd_base(), in_part);
                        if !locs.is_empty() {
                            return locs;
                        }
                    }
                }
            }
            Vec::new()
        } else {
            // rdd被pin住了，返回preferred_locations
            // when pinned, is required that there is exactly one preferred location
            // for a given partition
            assert!(rdd_prefs.len() == 1);
            rdd_prefs
        }
    }

    /// 在distributed_scheduler和local_scheduler中实现
    async fn get_shuffle_map_stage(&self, shuf: Arc<dyn ShuffleDependencyTrait>) -> Result<Stage>;
}

macro_rules! impl_common_scheduler_funcs {
    () => {
        #[inline]
        fn add_output_loc_to_stage(&self, stage_id: usize, partition: usize, host: String) {
            self.stage_cache
                .get_mut(&stage_id)
                .unwrap()
                .add_output_loc(partition, host);
        }

        #[inline]
        fn insert_into_stage_cache(&self, id: usize, stage: Stage) {
            self.stage_cache.insert(id, stage.clone());
        }

        #[inline]
        fn fetch_from_stage_cache(&self, id: usize) -> Stage {
            self.stage_cache.get(&id).unwrap().clone()
        }

        #[inline]
        fn fetch_from_shuffle_to_cache(&self, id: usize) -> Stage {
            self.shuffle_to_map_stage.get(&id).unwrap().clone()
        }

        #[inline]
        fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
            self.map_output_tracker
                .unregister_map_output(shuffle_id, map_id, server_uri)
        }

        #[inline]
        fn register_shuffle(&self, shuffle_id: usize, num_maps: usize) {
            self.map_output_tracker
                .register_shuffle(shuffle_id, num_maps)
        }

        #[inline]
        fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>) {
            self.map_output_tracker
                .register_map_outputs(shuffle_id, locs)
        }

        // Error: thread 'main' panicked at 'called `Option::unwrap()` on a `None` value'
        #[inline]
        fn remove_output_loc_from_stage(&self, shuffle_id: usize, map_id: usize, server_uri: &str) {
            self.shuffle_to_map_stage
                .get_mut(&shuffle_id)
                .unwrap() //获取stage
                .remove_output_loc(map_id, server_uri);
        }

        #[inline]
        fn get_cache_locs(&self, rdd: Arc<dyn RddBase>) -> Option<Vec<Vec<Ipv4Addr>>> {
            let locs_opt = self.cache_locs.get(&rdd.get_rdd_id());
            locs_opt.map(|l| l.clone())
        }

        #[inline]
        fn get_event_queue(&self) -> &Arc<DashMap<usize, VecDeque<CompletionEvent>>> {
            &self.event_queues
        }

        #[inline]
        fn get_next_job_id(&self) -> usize {
            self.next_job_id.fetch_add(1, Ordering::SeqCst)
        }

        #[inline]
        fn get_next_stage_id(&self) -> usize {
            self.next_stage_id.fetch_add(1, Ordering::SeqCst)
        }

        #[inline]
        fn get_next_task_id(&self) -> usize {
            self.next_task_id.fetch_add(1, Ordering::SeqCst)
        }
    };
}
