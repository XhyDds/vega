use std::fmt::Debug;
use std::fs;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::executor::{Executor, Signal};
use crate::io::ReaderConfiguration;
use crate::partial::{ApproximateEvaluator, PartialResult};
use crate::rdd::{ParallelCollection, Rdd, RddBase, UnionRdd};
use crate::scheduler::{DistributedScheduler, LocalScheduler, NativeScheduler, TaskContext};
use crate::serializable_traits::{Data, SerFunc};
use crate::serialized_data_capnp::serialized_data;
use crate::{env, hosts, utils, Fn, SerArc};
use log::error;
use once_cell::sync::OnceCell;
use simplelog::*;
use uuid::Uuid;
use Schedulers::*;

// There is a problem with this approach since T needs to satisfy PartialEq, Eq for Range
// No such restrictions are needed for Vec
//TOBE DONE
pub enum Sequence<T> {
    Range(Range<T>),
    Vec(Vec<T>),
}
/*Schedulers结构体
对local模式和distributed模式的scheduler封装得到的enum
用于中master中，用于管理workers
具体的local和distributed的定义取自"./scheduler"中
*/
#[derive(Clone)]
enum Schedulers {
    Local(Arc<LocalScheduler>),
    Distributed(Arc<DistributedScheduler>),
}
/*Schedulers默认的构造函数
默认local，max_failures=20，master=true
*/
impl Default for Schedulers {
    fn default() -> Schedulers {
        Schedulers::Local(Arc::new(LocalScheduler::new(20, true)))
    }
}
//TOBE DONE
impl Schedulers {
    ///scheduler的run_job函数
    /// 参数：
    ///    func            :* /
    /// final_rdd         :* /
    /// partitions        :* /
    /// allow_local       :传给distributed_scheduler和local_scheduler的参数
    fn run_job<T: Data, U: Data, F>(
        &self,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        let op_name = final_rdd.get_op_name();
        log::info!("starting `{}` job", op_name);
        let start = Instant::now();
        //按照自身的枚举类型分别处理
        match self {
            //distributed
            Distributed(distributed) => {
                //distributed_scheduler执行run_job
                //参数同scheduler的run_job保持一致
                let res = distributed
                    .clone()
                    .run_job(func, final_rdd, partitions, allow_local);
                log::info!(
                    "`{}` job finished, took {}s",
                    op_name,
                    start.elapsed().as_secs()
                );
                res
            }
            //local
            Local(local) => {
                //local_scheduler执行run_job
                //参数同scheduler的run_job保持一致
                let res = local
                    .clone()
                    .run_job(func, final_rdd, partitions, allow_local);
                log::info!(
                    "`{}` job finished, took {}s",
                    op_name,
                    start.elapsed().as_secs()
                );
                res
            }
        }
    }

    fn run_approximate_job<T: Data, U: Data, R, F, E>(
        &self,
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
        let op_name = final_rdd.get_op_name();
        log::info!("starting `{}` job", op_name);
        let start = Instant::now();
        let res = match self {
            Distributed(distributed) => distributed
                .clone()
                .run_approximate_job(func, final_rdd, evaluator, timeout),
            Local(local) => local
                .clone()
                .run_approximate_job(func, final_rdd, evaluator, timeout),
        };
        log::info!(
            "`{}` job finished, took {}s",
            op_name,
            start.elapsed().as_secs()
        );
        res
    }
}
/**************************************************************************************/
/* 结构体：Context
含义：
成员：
    next_rdd_id         :* /
                        ** 从0开始
    next_shuffle_id     :* /
                        ** 从0开始
    scheduler           :* /
    address_map         :* socket_address数组
                        ** 单机：127.0.0.1:0 多机：ip+port
    distributed_driver  :* /
    work_dir            :* /
                        ** Configuration.local_dir+"ns-session-"+job_id
补充：AtomicUsize：An integer type which can be safely shared between threads.
*/
#[derive(Default)]
pub struct Context {
    next_rdd_id: Arc<AtomicUsize>,
    next_shuffle_id: Arc<AtomicUsize>,
    scheduler: Schedulers,
    pub(crate) address_map: Vec<SocketAddrV4>,
    distributed_driver: bool,
    /// this context/session temp work dir
    work_dir: PathBuf,
}

/*Context超出作用域时的析构函数
即，程序结束运行时的清理（正常退出）
打印log信息+调用driver_clean_up_directives清理目录
*/
impl Drop for Context {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        {
            let deployment_mode = env::Configuration::get().deployment_mode;
            if self.distributed_driver && deployment_mode == env::DeploymentMode::Distributed {
                log::info!("inside context drop in master");
            } else if deployment_mode == env::DeploymentMode::Distributed {
                log::info!("inside context drop in executor");
            }
        }
        Context::driver_clean_up_directives(&self.work_dir, &self.address_map);
    }
}
//TOBE DONE
impl Context {
    //new
    pub fn new() -> Result<Arc<Self>> {
        //根据模式创建context
        //有关hdfs的配置
        // std::env::set_var("JAVA_HOME", "/home/lml/.jdk/jdk1.8.0_371");
        // std::env::set_var("HADOOP_HOME", "/home/lml/hadoop-3.3.5");

        let mut file = std::fs::File::create("/tmp/env1.txt").expect("create failed");
        for (key, value) in std::env::vars() {
            let msg = format!("{}: {}\n", key, value);
            file.write(msg.as_bytes()).expect("write failed");
        }
        //创建context
        match Context::with_mode(env::Configuration::get().deployment_mode) {
            Ok(ctx) => {
                log::info!("context created");
                println!("context created");
                Ok(ctx)
            }
            Err(err) => {
                log::error!("failed to create context: {}", err);
                Err(err)
            }
        }
    }

    /*函数with_mode
    根据模式创建context
    from: env::Configuration.deployment_mode
    */
    pub fn with_mode(mode: env::DeploymentMode) -> Result<Arc<Self>> {
        match mode {
            //分布式
            env::DeploymentMode::Distributed => {
                if env::Configuration::get().is_driver {
                    //master
                    let ctx = Context::init_distributed_driver()?;
                    ctx.set_cleanup_process();
                    Ok(ctx)
                } else {
                    //slave
                    Context::init_distributed_worker()?
                }
            }
            //本地
            env::DeploymentMode::Local => Context::init_local_scheduler(),
        }
    }

    /// Sets a handler to receives any external signal to stop the process
    /// and shuts down gracefully any ongoing op
    /// 一旦ctrl c ，即开始driver_clean_up_directives（非正常退出）
    fn set_cleanup_process(&self) {
        let address_map = self.address_map.clone();
        let work_dir = self.work_dir.clone();
        //在master上开协程，监测ctrl-c，并在ctrl-c时优雅停机
        env::Env::run_in_async_rt(|| {
            tokio::spawn(async move {
                // avoid moving a self clone here or drop won't be potentially called
                // before termination and never clean up
                if tokio::signal::ctrl_c().await.is_ok() {
                    log::info!("received termination signal, cleaning up");
                    Context::driver_clean_up_directives(&work_dir, &address_map);
                    std::process::exit(0);
                }
            });
        })
    }
    /*方法：init_local_scheduler
    用于本地生成context
    schedule: Schedulers::Local(Arc::new(LocalScheduler::new(20, true)))
    */
    fn init_local_scheduler() -> Result<Arc<Self>> {
        //生成无重复的任务id
        let job_id = Uuid::new_v4().to_string();
        //创建任务工作目录（在Configuration的local_dir下）
        let job_work_dir = env::Configuration::get()
            .local_dir
            .join(format!("ns-session-{}", job_id));
        fs::create_dir_all(&job_work_dir).unwrap();
        //创建loggers
        initialize_loggers(job_work_dir.join("ns-driver.log"));
        //创建scheduler
        //同默认配置：local，max_failures=20，master=true
        let scheduler = Schedulers::Local(Arc::new(LocalScheduler::new(20, true)));

        //LOCALHOST：127.0.0.1
        Ok(Arc::new(Context {
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            scheduler,
            address_map: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)],
            distributed_driver: false,
            work_dir: job_work_dir,
        }))
    }

    /// Initialization function for the application driver.
    /// * Distributes the configuration setup to the workers.
    /// * Distributes a copy of the application binary to all the active worker host nodes.
    /// * Launches the workers in the remote machine using the same binary (required).
    /// * Creates and returns a working Context.
    /// * 端口从10000开始，以5000为步长递增
    fn init_distributed_driver() -> Result<Arc<Self>> {
        let mut port: u16 = 10000;
        let mut address_map = Vec::new();
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = env::Configuration::get()
            .local_dir
            .join(format!("ns-session-{}", job_id));
        let job_work_dir_str = job_work_dir
            .to_str()
            .ok_or_else(|| Error::PathToString(job_work_dir.clone()))?;
        println!("work_dir:{}", job_work_dir_str);
        let binary_path = std::env::current_exe().map_err(|_| Error::CurrentBinaryPath)?;
        let binary_path_str = binary_path
            .to_str()
            .ok_or_else(|| Error::PathToString(binary_path.clone()))?;
        let binary_name = binary_path
            .file_name()
            .ok_or(Error::CurrentBinaryName)?
            .to_os_string()
            .into_string()
            .map_err(Error::OsStringToString)?;

        fs::create_dir_all(&job_work_dir).unwrap();
        let conf_path = job_work_dir.join("config.toml");
        let conf_path = conf_path.to_str().unwrap();
        //创建logger
        initialize_loggers(job_work_dir.join("ns-driver.log"));

        if let Some(slaves) = &hosts::Hosts::get()?.slaves {
            //对每个slave进行配置
            for slave in slaves {
                let address = &slave.ip;
                let key_path = match &slave.key {
                    Some(key) => &key,
                    None => "~/.ssh/id_rsa",
                };
                log::info!("key_path:{}", key_path);
                log::debug!("deploying executor at address {:?}", address);
                let address_ip: Ipv4Addr = address
                    .split('@')
                    .nth(1)
                    .ok_or_else(|| Error::ParseHostAddress(address.into()))?
                    .parse()
                    .map_err(|x| Error::ParseHostAddress(format!("{}", x)))?;
                address_map.push(SocketAddrV4::new(address_ip, port));

                let work_dir = format!(
                    "{}{}",
                    "/home/",
                    slave.ip.as_str().split('@').nth(0).unwrap()
                );
                //配置java_home
                match &slave.java_home {
                    Some(java_home) => {
                        Command::new("ssh")
                            .args(&[
                                "-i",
                                key_path,
                                address,
                                "export",
                                format!("JAVA_HOME={}", java_home).as_str(),
                            ])
                            .output()
                            .map_err(|e| Error::CommandOutput {
                                source: e,
                                command: "ssh export".into(),
                            })?;
                        log::info!("java_path:{}", java_home);
                    }
                    None => {}
                };
                //配置hadoop_home
                match &slave.hadoop_home {
                    Some(hadoop_home) => {
                        Command::new("ssh")
                            .args(&[
                                "-i",
                                key_path,
                                address,
                                "export",
                                format!("HADOOP_HOME={}", hadoop_home).as_str(),
                            ])
                            .output()
                            .map_err(|e| Error::CommandOutput {
                                source: e,
                                command: "ssh export".into(),
                            })?;
                        log::info!("hadoop_path:{}", hadoop_home);
                    }
                    None => {}
                };

                Command::new("ssh")
                    .args(&["-i", key_path, address, "mkdir", &job_work_dir_str])
                    .output()
                    .map_err(|e| Error::CommandOutput {
                        source: e,
                        command: "ssh mkdir".into(),
                    })?;

                // Copy conf file to remote:
                //创建worker_config(config.toml)
                Context::create_workers_config_file(address_ip, port, conf_path)?;
                let remote_path = format!("{}:{}/config.toml", address, job_work_dir_str);
                Command::new("scp")
                    .args(&["-i", key_path, conf_path, &remote_path])
                    .output()
                    .map_err(|e| Error::CommandOutput {
                        source: e,
                        command: "scp config".into(),
                    })?;

                // Copy binary:
                //赋值二进制文件到slave中
                let remote_path = format!("{}:{}/{}", address, job_work_dir_str, binary_name);
                log::debug!("{}", binary_path_str);
                log::debug!("{}", remote_path);
                Command::new("scp")
                    .args(&["-i", key_path, &binary_path_str, &remote_path])
                    .output()
                    .map_err(|e| Error::CommandOutput {
                        source: e,
                        command: "scp executor".into(),
                    })?;

                // Copy hosts.conf:
                // 赋值hosts.conf到slave/~/中(强制覆盖)
                let hosts_path = std::env::home_dir()
                    .ok_or(Error::NoHome)?
                    .join("hosts.conf");
                let hosts_path_str = hosts_path.to_str().unwrap();
                let remote_hosts_path = format!("{}:{}/hosts.conf", address, work_dir);
                println!("{}", hosts_path_str);
                println!("{}", remote_hosts_path);
                Command::new("scp")
                    .args(&["-f", "-i", key_path, &hosts_path_str, &remote_hosts_path])
                    .output()
                    .map_err(|e| Error::CommandOutput {
                        source: e,
                        command: "scp executor".into(),
                    })?;

                // Deploy a remote slave:
                //通过ssh控制slave，run程序
                let path = format!("{}/{}", job_work_dir_str, binary_name);
                log::debug!("remote path {}", path);
                Command::new("ssh")
                    .args(&["-i", key_path, address, &path])
                    .spawn()
                    .map_err(|e| Error::CommandOutput {
                        source: e,
                        command: "ssh run".into(),
                    })?;
                port += 5000;
            }
        } else {
            return Err(Error::NoSlaves);
        };

        //此处scheduler采用ditributed模式，特别的参数是port=10000//WHY TOBE DONE
        Ok(Arc::new(Context {
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            scheduler: Schedulers::Distributed(Arc::new(DistributedScheduler::new(
                20,
                true,
                Some(address_map.clone()),
                10000,
            ))),
            address_map,
            distributed_driver: true,
            work_dir: job_work_dir,
        }))
    }
    /*函数init_distributed_worker
    初始化worker
    包含创建logger，executor，并等待executor执行结束后，开始清理目录
    */
    fn init_distributed_worker() -> Result<!> {
        let mut work_dir = PathBuf::from("");
        match std::env::current_exe().map_err(|_| Error::CurrentBinaryPath) {
            Ok(binary_path) => {
                match binary_path.parent().ok_or_else(|| Error::CurrentBinaryPath) {
                    Ok(dir) => work_dir = dir.into(),
                    //如果失败，worker立即下线
                    Err(err) => Context::worker_clean_up_directives(Err(err), work_dir)?,
                };
                //logger
                initialize_loggers(work_dir.join("ns-executor.log"));
            }
            //如果失败，worker立即下线
            Err(err) => Context::worker_clean_up_directives(Err(err), work_dir)?,
        }

        log::debug!("starting worker");
        //获取port
        let port = match env::Configuration::get()
            .slave
            .as_ref()
            .map(|c| c.port)
            .ok_or(Error::GetOrCreateConfig("executor port not set"))
        {
            Ok(port) => port,
            Err(err) => Context::worker_clean_up_directives(Err(err), work_dir)?,
        };
        //创建executor，port为master指定的port
        let executor = Arc::new(Executor::new(port));
        //worker结束运行后执行清理目录操作//TOBE DONE(worker如何运行)
        Context::worker_clean_up_directives(executor.worker(), work_dir)
    }

    /*函数driver_clean_up_directives
    清理目录，结束运行（slave使用）
    */
    fn worker_clean_up_directives(run_result: Result<Signal>, work_dir: PathBuf) -> Result<!> {
        env::Env::get().shuffle_manager.clean_up_shuffle_data();
        utils::clean_up_work_dir(&work_dir);
        match run_result {
            Err(err) => {
                log::error!("executor failed with error: {}", err);
                std::process::exit(1);
            }
            Ok(value) => {
                log::info!("executor closed gracefully with signal: {:?}", value);
                std::process::exit(0);
            }
        }
    }

    /*函数driver_clean_up_directives
    清理目录，结束运行（master使用）
    适用于local和distributed两种模式
    distributed模式额外控制slaves下线
    */
    fn driver_clean_up_directives(work_dir: &Path, executors: &[SocketAddrV4]) {
        //master控制slaves下线（只在分布式下）
        Context::drop_executors(executors);
        // Give some time for the executors to shut down and clean up
        std::thread::sleep(std::time::Duration::from_millis(1_500));
        //slaves下线完毕后清理shuffle_data//TOBE DONE(lhm)
        env::Env::get().shuffle_manager.clean_up_shuffle_data();
        //清理工作区
        utils::clean_up_work_dir(work_dir);
    }

    /*函数create_workers_config_file
    为workers创建对应的conf.toml配置文件
    以master的config为基础，修改local_ip为自机ip，slave为init_distributed_driver处设置的端口，is_driver为false
     */
    fn create_workers_config_file(local_ip: Ipv4Addr, port: u16, config_path: &str) -> Result<()> {
        let mut current_config = env::Configuration::get().clone();
        current_config.local_ip = local_ip;
        current_config.slave = Some(std::convert::From::<(bool, u16)>::from((true, port)));
        current_config.is_driver = false;

        let config_string = toml::to_string_pretty(&current_config).unwrap();
        let mut config_file = fs::File::create(config_path).unwrap();
        config_file.write_all(config_string.as_bytes()).unwrap();
        Ok(())
    }

    /*函数drop_executors
    master中运行，只在分布式下运行
    向slaves发送ShutDownGracefully信号
    */
    fn drop_executors(address_map: &[SocketAddrV4]) {
        //只在distributed模式下运行
        if env::Configuration::get().deployment_mode.is_local() {
            return;
        }

        //遍历slaves
        for socket_addr in address_map {
            log::debug!(
                "dropping executor in {:?}:{:?}",
                socket_addr.ip(),
                socket_addr.port()
            );
            if let Ok(mut stream) =
                TcpStream::connect(format!("{}:{}", socket_addr.ip(), socket_addr.port() + 10))
            {
                //对TcpStream发送ShutDownGracefully信号
                let signal = bincode::serialize(&Signal::ShutDownGracefully).unwrap();
                let mut message = capnp::message::Builder::new_default();
                let mut task_data = message.init_root::<serialized_data::Builder>();
                task_data.set_msg(&signal);
                capnp::serialize::write_message(&mut stream, &message)
                    .map_err(Error::OutputWrite)
                    .unwrap();
            } else {
                error!(
                    "Failed to connect to {}:{} in order to stop its executor",
                    socket_addr.ip(),
                    socket_addr.port()
                );
            }
        }
    }

    /// 获取新的rdd_id并更新rdd_id
    pub fn new_rdd_id(self: &Arc<Self>) -> usize {
        self.next_rdd_id.fetch_add(1, Ordering::SeqCst)
    }

    /// 获取新的shuffle_id并更新shuffle_id
    pub fn new_shuffle_id(self: &Arc<Self>) -> usize {
        self.next_shuffle_id.fetch_add(1, Ordering::SeqCst)
        //Ordering::SeqCst是一个原子内存序，保证多线程下对内存的操作顺序是正确顺序
        //fetch_add函数是存入新值并返回上一个值（Adds to the current value, returning the previous value.）
    }

    pub fn make_rdd<T: Data, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> SerArc<dyn Rdd<Item = T>>
    where
        I: IntoIterator<Item = T>,
    {
        //seq是迭代器，num_slices是分区数量
        let rdd = self.parallelize(seq, num_slices);
        // parallelize  可以将 seq data按num_slices划分，产生rdd
        rdd.register_op_name("make_rdd");
        rdd
    }

    pub fn range(
        self: &Arc<Self>,
        start: u64,
        end: u64,
        step: usize,
        num_slices: usize,
    ) -> SerArc<dyn Rdd<Item = u64>> {
        // TODO: input validity check
        let seq = (start..=end).step_by(step);
        let rdd = self.parallelize(seq, num_slices);
        rdd.register_op_name("range");
        rdd
    }

    pub fn parallelize<T: Data, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> SerArc<dyn Rdd<Item = T>>
    where
        I: IntoIterator<Item = T>,
    {
        //函数功能是将seq建为一个有num_slices个分区的RDD

        //SerArc是一个基于Serde的Rust库，它提供了一种将Rust结构体序列化为Arc的方法。
        /*Serde是一个Rust库，用于序列化和反序列化Rust数据结构。
         * 它支持以下数据类型的序列化和反序列化，
         * 包括String、&str、usize、Vec<T>和HashMap<K,V>。
         * 此外，Serde还提供了derive macro来
         * 为你自己定义的数据类型提供序列化和反序列化的实现
         * 包含JSON,Pickle,URL,TOML等 */
        //将由context,data,num_slices创建的ParallelCollection序列化为Arc(方便在executor间传递，功能：rdd)，并返回
        SerArc::new(ParallelCollection::new(self.clone(), seq, num_slices))
    }

    /// Load from a distributed source and turns it into a parallel collection.
    pub fn read_source<F, C, I: Data, O: Data>(
        self: &Arc<Self>,
        config: C,
        func: F,
    ) -> impl Rdd<Item = O>
    where
        F: SerFunc(I) -> O,
        C: ReaderConfiguration<I>,
    {
        config.make_reader(self.clone(), func)
    }

    pub fn run_job<T: Data, U: Data, F>(
        //对给定的RDD进行F操作
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> Result<Vec<U>>
    where
        F: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
    {
        //对函数进行包装，使其可序列化可传输
        let cl = Fn!(move |(_task_context, iter)| (func)(iter));
        let func = Arc::new(cl);
        //将job交给scheduler执行
        //默认禁止本地
        self.scheduler.run_job(
            func,
            rdd.clone(),
            (0..rdd.number_of_splits()).collect(),
            false,
            //true,
        )
    }

    pub fn run_job_with_partitions<T: Data, U: Data, F, P>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
        partitions: P,
    ) -> Result<Vec<U>>
    where
        F: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
        P: IntoIterator<Item = usize>,
    {
        let cl = Fn!(move |(_task_context, iter)| (func)(iter));
        self.scheduler
            .run_job(Arc::new(cl), rdd, partitions.into_iter().collect(), false)
    }

    pub fn run_job_with_context<T: Data, U: Data, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        log::debug!("inside run job in context");
        let func = Arc::new(func);
        self.scheduler.run_job(
            func,
            rdd.clone(),
            (0..rdd.number_of_splits()).collect(),
            false,
        )
    }

    /// Run a job that can return approximate results. Returns a partial result
    /// (how partial depends on whether the job was finished before or after timeout).
    pub(crate) fn run_approximate_job<T: Data, U: Data, R, F, E>(
        self: &Arc<Self>,
        func: F,
        rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: Duration,
    ) -> Result<PartialResult<R>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        self.scheduler
            .run_approximate_job(Arc::new(func), rdd, evaluator, timeout)
    }

    pub(crate) fn get_preferred_locs(
        &self,
        rdd: Arc<dyn RddBase>,
        partition: usize,
    ) -> Vec<std::net::Ipv4Addr> {
        match &self.scheduler {
            Schedulers::Distributed(scheduler) => scheduler.get_preferred_locs(rdd, partition),
            Schedulers::Local(scheduler) => scheduler.get_preferred_locs(rdd, partition),
        }
    }

    pub fn union<T: Data>(rdds: &[Arc<dyn Rdd<Item = T>>]) -> Result<impl Rdd<Item = T>> {
        UnionRdd::new(rdds)
    }
}
//LOGGER
static LOGGER: OnceCell<()> = OnceCell::new();
/*函数initialize_loggers
功能：在file_path下创建log文件
TOBE DONE
*/
fn initialize_loggers<P: Into<PathBuf>>(file_path: P) {
    fn _initializer(file_path: PathBuf) {
        let log_level = env::Configuration::get().loggin.log_level.into();
        log::info!("path for file logger: {}", file_path.display());
        let file_logger: Box<dyn SharedLogger> = WriteLogger::new(
            log_level,
            Config::default(),
            fs::File::create(file_path).expect("not able to create log file"),
        );
        let mut combined = vec![file_logger];
        if let Some(term_logger) =
            TermLogger::new(log_level, Config::default(), TerminalMode::Mixed)
        {
            let logger: Box<dyn SharedLogger> = term_logger;
            combined.push(logger);
        }
        CombinedLogger::init(combined).unwrap();
    }

    LOGGER.get_or_init(move || _initializer(file_path.into()));
}
